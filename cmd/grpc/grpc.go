package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/CSKU-Lab/task-service/configs"
	graderPB "github.com/CSKU-Lab/task-service/genproto/grader/v1"
	pb "github.com/CSKU-Lab/task-service/genproto/task/v1"
	"github.com/CSKU-Lab/task-service/internal/logging"
	"github.com/CSKU-Lab/task-service/models"
	"github.com/CSKU-Lab/task-service/mongodb"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	logger, loggerCleanup, err := logging.New(os.Getenv("ENV"))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := loggerCleanup(); err != nil {
			logger.Warnw("failed to flush logger", "error", err)
		}
	}()

	env := configs.NewEnv()

	client, err := mongo.Connect(options.Client().ApplyURI(env.Get("MONGO_URI")))
	if err != nil {
		logger.Fatalw("Failed to connect to MongoDB", "error", err)
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		logger.Fatalw("Failed to ping MongoDB", "error", err)
	}

	db := client.Database(env.Get("DATABASE_NAME"))

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", env.Get("PORT")))
	if err != nil {
		logger.Fatalw("Failed to listen", "error", err)
	}

	graderClient, closeGraderClient := initGraderServerClient(logger, env.Get("GRADER_SERVER_URL"))
	defer closeGraderClient()

	s := grpc.NewServer()
	pb.RegisterTaskServiceServer(s, NewGRPCServer(db, logger, graderClient))
	reflection.Register(s)
	logger.Infow("gRPC TaskService registered")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		sig := <-sigs
		logger.Infow("Received shutdown signal", "signal", sig)
		timer := time.AfterFunc(10*time.Second, func() {
			logger.Warn("Server couldn't stop gracefully in time. Doing force stop.")
		})
		defer timer.Stop()

		s.GracefulStop()
		logger.Info("Server gracefully stopped")
	}()

	if err := s.Serve(lis); err != nil {
		logger.Fatalw("Cannot start gRPC server", "error", err)
	}

	wg.Wait()
	logger.Info("Server shutdown complete")
}

type grpcServer struct {
	pb.UnimplementedTaskServiceServer
	db           *mongo.Database
	logger       *zap.SugaredLogger
	graderClient graderPB.GraderServiceClient
}

func NewGRPCServer(db *mongo.Database, logger *zap.SugaredLogger, graderClient graderPB.GraderServiceClient) pb.TaskServiceServer {
	return &grpcServer{
		db:           db,
		logger:       logger,
		graderClient: graderClient,
	}
}

// this method is only use for postman maybe no need to use in production
// because in main service we already pagination there and just receive task_id from there and query just only that ids is enough
func (g *grpcServer) GetTasks(ctx context.Context, req *pb.GetTasksRequest) (*pb.GetTasksResponse, error) {
	cursor, err := g.db.Collection("tasks").Find(ctx, bson.D{})
	if err != nil {
		g.logger.Errorw("Failed to find tasks", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to find tasks: %v", err)
	}
	defer cursor.Close(ctx)

	var tasks []*pb.TaskResponse
	for cursor.Next(ctx) {
		var task models.Task
		if err := cursor.Decode(&task); err != nil {
			g.logger.Errorw("Failed to decode task", "error", err)
			return nil, status.Errorf(codes.Internal, "failed to decode task: %v", err)
		}

		taskLimit := &pb.Limit{}
		if task.Limit != nil {
			taskLimit = &pb.Limit{
				CpuTime:      task.Limit.CpuTime,
				CpuExtraTime: task.Limit.CpuExtraTime,
				WallTime:     task.Limit.WallTime,
				Memory:       task.Limit.Memory,
				Stack:        task.Limit.Stack,
				MaxOpenFiles: task.Limit.MaxOpenFiles,
				MaxFileSize:  task.Limit.MaxFileSize,
				NetworkAllow: task.Limit.NetworkAllow,
			}
		}

		solutionFiles := make([]*pb.SolutionFile, len(task.SolutionFiles))
		for i, file := range task.SolutionFiles {
			solutionFiles[i] = &pb.SolutionFile{
				Name:    file.Name,
				Content: file.Content,
			}
		}

		testcases := make([]*pb.TestCase, len(task.TestCases))
		for i, testcase := range task.TestCases {
			testcases[i] = &pb.TestCase{
				Order:  testcase.Order,
				Input:  testcase.Input,
				Output: testcase.Output,
			}
		}

		taskRes := &pb.TaskResponse{
			Id:               task.ID,
			AllowedRunnerIds: task.AllowedRunnerIDs,
			CompareScriptId:  task.CompareID,
			Limit:            taskLimit,
			Testcases:        testcases,
			SolutionFiles:    solutionFiles,
			SolutionRunnerId: task.SolutionRunnerID,
		}

		tasks = append(tasks, taskRes)
	}

	g.logger.Infow("Retrieved tasks", "count", len(tasks))
	return &pb.GetTasksResponse{Tasks: tasks}, nil
}

func (g *grpcServer) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.TaskResponse, error) {
	taskID := req.GetId()
	if taskID == "" {
		return nil, status.Error(codes.InvalidArgument, "task id is required")
	}

	task, err := g.getTask(ctx, taskID)
	if err != nil {
		return nil, err
	}

	taskLimit := &pb.Limit{}
	if task.Limit != nil {
		taskLimit = &pb.Limit{
			CpuTime:      task.Limit.CpuTime,
			CpuExtraTime: task.Limit.CpuExtraTime,
			WallTime:     task.Limit.WallTime,
			Memory:       task.Limit.Memory,
			Stack:        task.Limit.Stack,
			MaxOpenFiles: task.Limit.MaxOpenFiles,
			MaxFileSize:  task.Limit.MaxFileSize,
			NetworkAllow: task.Limit.NetworkAllow,
		}
	}

	solutionFiles := make([]*pb.SolutionFile, len(task.SolutionFiles))
	for i, file := range task.SolutionFiles {
		solutionFiles[i] = &pb.SolutionFile{
			Name:    file.Name,
			Content: file.Content,
		}
	}

	testcases := make([]*pb.TestCase, len(task.TestCases))
	for i, testcase := range task.TestCases {
		testcases[i] = &pb.TestCase{
			Order:  testcase.Order,
			Input:  testcase.Input,
			Output: testcase.Output,
		}
	}

	g.logger.Infow("Retrieved task", "taskId", taskID)
	return &pb.TaskResponse{
		Id:               task.ID,
		AllowedRunnerIds: task.AllowedRunnerIDs,
		CompareScriptId:  task.CompareID,
		Testcases:        testcases,
		Limit:            taskLimit,
		SolutionFiles:    solutionFiles,
		SolutionRunnerId: task.SolutionRunnerID,
	}, nil
}

func (g *grpcServer) CreateTask(ctx context.Context, req *emptypb.Empty) (*pb.CreateTaskResponse, error) {
	id, err := uuid.NewV7()
	if err != nil {
		g.logger.Errorw("Failed to generate UUID", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to generate UUID: %v", err)
	}

	_, err = g.db.Collection("tasks").InsertOne(ctx, bson.M{"_id": id.String()})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create task: %v", err)
	}

	return &pb.CreateTaskResponse{
		Id: id.String(),
	}, nil
}

func (g *grpcServer) UpdateTask(ctx context.Context, req *pb.UpdateTaskRequest) (*emptypb.Empty, error) {
	var limit *models.Limit
	if req.GetLimit() != nil {
		limit = &models.Limit{
			CpuTime:      req.GetLimit().GetCpuTime(),
			CpuExtraTime: req.GetLimit().GetCpuExtraTime(),
			WallTime:     req.GetLimit().GetWallTime(),
			Memory:       req.GetLimit().GetMemory(),
			Stack:        req.GetLimit().GetStack(),
			MaxOpenFiles: req.GetLimit().GetMaxOpenFiles(),
			MaxFileSize:  req.GetLimit().GetMaxFileSize(),
			NetworkAllow: req.GetLimit().GetNetworkAllow(),
		}
	}

	updatedFields := mongodb.GetUpdatedFields(&models.UpdateTask{
		AllowedRunnerIDs: req.AllowedRunnerIds,
		CompareID:        req.CompareScriptId,
		Limit:            limit,
		SolutionRunnerID: req.SolutionRunnerId,
	})

	// re generate test cases if solution files, solution runner id or test cases are updated
	if req.GetSolutionFiles() != nil || req.GetSolutionRunnerId() != "" || req.GetTestcases() != nil || req.GetLimit() != nil {
		task, err := g.getTask(ctx, req.GetId())
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return nil, status.Error(codes.NotFound, "task not found")
			}
			return nil, err
		}

		if limit == nil {
			limit = task.Limit
		}

		var graderLimit *graderPB.Limit
		if limit != nil {
			graderLimit = &graderPB.Limit{
				CpuTime:      limit.CpuTime,
				CpuExtraTime: limit.CpuExtraTime,
				WallTime:     limit.WallTime,
				Memory:       limit.Memory,
				Stack:        limit.Stack,
				MaxOpenFiles: limit.MaxOpenFiles,
				MaxFileSize:  limit.MaxFileSize,
				NetworkAllow: limit.NetworkAllow,
			}
		}

		solutionFiles := task.SolutionFiles
		if req.GetSolutionFiles() != nil {
			newSolutionFiles := make([]models.SolutionFile, 0, len(req.GetSolutionFiles()))
			for _, file := range req.GetSolutionFiles() {
				newSolutionFiles = append(newSolutionFiles, models.SolutionFile{
					Name:    file.GetName(),
					Content: file.GetContent(),
				})

			}

			solutionFiles = newSolutionFiles
			updatedFields["solution_files"] = newSolutionFiles
		}

		graderSolutionFiles := make([]*graderPB.File, len(solutionFiles))
		for i, file := range solutionFiles {
			graderSolutionFiles[i] = &graderPB.File{
				Name:    file.Name,
				Content: file.Content,
			}
		}

		testcases := task.TestCases
		if req.GetTestcases() != nil {
			newTestcases := make([]models.TestCase, len(req.GetTestcases()))
			for i, testcase := range req.GetTestcases() {
				newTestcases[i] = models.TestCase{
					Order: testcase.GetOrder(),
					Input: testcase.GetInput(),
				}
			}
			testcases = newTestcases
		}

		solutionRunnerId := task.SolutionRunnerID
		if req.GetSolutionRunnerId() != "" {
			solutionRunnerId = req.SolutionRunnerId
		}

		if solutionRunnerId == nil && len(testcases) > 0 {
			return nil, status.Error(codes.InvalidArgument, "solution runner id is required to generate test cases")
		}

		if len(solutionFiles) == 0 && len(testcases) > 0 {
			return nil, status.Error(codes.InvalidArgument, "solution files are required to generate test cases")
		}

		if len(testcases) > 0 {
			graderTestcases := make([]*graderPB.TestCaseRequest, len(testcases))
			for i, testcase := range testcases {
				graderTestcases[i] = &graderPB.TestCaseRequest{
					Order: testcase.Order,
					Input: testcase.Input,
				}
			}

			res, err := g.graderClient.GenerateTestCases(ctx, &graderPB.GenerateTestCasesRequest{
				Files:     graderSolutionFiles,
				Testcases: graderTestcases,
				RunnerId:  *solutionRunnerId,
				Limit:     graderLimit,
			})
			if err != nil {
				return nil, status.Errorf(codes.Internal, "failed to generate test cases: %v", err)
			}

			updateTestCases := make([]models.TestCase, 0, len(res.GetResults()))
			for _, gt := range res.GetResults() {
				updateTestCases = append(updateTestCases, models.TestCase{
					Order:  gt.GetOrder(),
					Input:  gt.GetInput(),
					Output: gt.GetOutput(),
				})
			}

			updatedFields["testcases"] = updateTestCases
		}

	}

	_, err := g.db.Collection("tasks").UpdateByID(ctx, req.GetId(), bson.D{{Key: "$set", Value: updatedFields}})
	if err != nil {
		g.logger.Errorw("Failed to upsert task", "error", err, "taskId", req.GetId())
		return nil, status.Errorf(codes.Internal, "failed to upsert task: %v", err)
	}

	g.logger.Infow("Task updated", "taskId", req.GetId())
	return nil, nil
}

func (g *grpcServer) DeleteTask(ctx context.Context, req *pb.DeleteTaskRequest) (*emptypb.Empty, error) {
	taskID := req.GetId()
	if taskID == "" {
		return nil, status.Error(codes.InvalidArgument, "task id is required")
	}

	_, err := g.db.Collection("tasks").DeleteOne(ctx, bson.M{"_id": taskID})
	if err != nil {
		g.logger.Errorw("Failed to delete task", "error", err, "taskId", taskID)
		return nil, status.Errorf(codes.Internal, "failed to delete task: %v", err)
	}

	g.logger.Infow("Task deleted", "taskId", taskID)
	return &emptypb.Empty{}, nil
}

func (g *grpcServer) getTask(ctx context.Context, id string) (*models.Task, error) {
	var task models.Task
	err := g.db.Collection("tasks").FindOne(ctx, bson.M{"_id": id}).Decode(&task)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, err
		}

		g.logger.Errorw("Failed to find task", "error", err, "taskId", id)
		return nil, status.Errorf(codes.Internal, "failed to find task: %v", err)
	}

	return &task, nil
}

func initGraderServerClient(logger *zap.SugaredLogger, clientAddr string) (client graderPB.GraderServiceClient, close func()) {
	conn, err := grpc.NewClient(clientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatalf("Failed to connect to gRPC server: %v", err)
	}

	c := graderPB.NewGraderServiceClient(conn)

	return c, func() {
		conn.Close()
	}
}
