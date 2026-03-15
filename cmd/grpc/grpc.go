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

	"github.com/CSKU-Lab/cache"
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
	"golang.org/x/sync/errgroup"
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

	redis, err := cache.NewRedis(&cache.RedisOptions{
		Addr:     env.Get("REDIS_SERVER_URL"),
		Password: env.Get("REDIS_PASSWORD"),
	})
	if err != nil {
		logger.Fatalw("Cannot initialize cache repository", "error", err)
	}
	defer redis.Close()

	s := grpc.NewServer()
	pb.RegisterTaskServiceServer(s, NewGRPCServer(db, logger, graderClient, redis))
	reflection.Register(s)
	logger.Infow("gRPC TaskService registered")

	var wg sync.WaitGroup

	wg.Go(func() {
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
	})

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
	taskCache    cache.CacheBuild
	graderClient graderPB.GraderServiceClient
	cacheApp     cache.CacheApp
}

func NewGRPCServer(db *mongo.Database, logger *zap.SugaredLogger, graderClient graderPB.GraderServiceClient, cacheApp cache.CacheApp) pb.TaskServiceServer {
	taskCache := cacheApp.Build("taskCache")

	return &grpcServer{
		db:           db,
		logger:       logger,
		taskCache:    taskCache,
		graderClient: graderClient,
		cacheApp:     cacheApp,
	}
}

// this method is only use for postman maybe no need to use in production
// because in main service we already pagination there and just receive task_id from there and query just only that ids is enough
func (g *grpcServer) GetTasks(ctx context.Context, req *pb.GetTasksRequest) (*pb.GetTasksResponse, error) {
	cacheObj := g.taskCache.All(time.Hour * 4)
	cacheInstance := cache.NewCacheInstance[*pb.GetTasksResponse](cacheObj)

	taskRes, err := cacheInstance.LazyCaching(ctx, func() (*pb.GetTasksResponse, error) {
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

			tasks = append(tasks, taskModelToPB(&task))
		}

		return &pb.GetTasksResponse{Tasks: tasks}, nil
	})
	if err != nil {
		return nil, err
	}
	return taskRes, nil
}

func (g *grpcServer) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.TaskResponse, error) {
	taskID := req.GetId()
	if taskID == "" {
		return nil, status.Error(codes.InvalidArgument, "task id is required")
	}

	cacheObj := g.taskCache.One(time.Hour*4, req.GetId())
	cacheInstance := cache.NewCacheInstance[*pb.TaskResponse](cacheObj)

	taskRes, err := cacheInstance.LazyCaching(ctx, func() (*pb.TaskResponse, error) {
		task, err := g.getTask(ctx, taskID)
		if err != nil {
			return nil, err
		}

		return taskModelToPB(task), nil
	})
	if err != nil {
		return nil, err
	}
	return taskRes, nil
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

	err = g.taskCache.InvalidateAll(ctx)
	if err != nil {
		return nil, err
	}

	return &pb.CreateTaskResponse{
		Id: id.String(),
	}, nil
}

func (g *grpcServer) UpdateTask(ctx context.Context, req *pb.UpdateTaskRequest) (*emptypb.Empty, error) {
	// Map allowed runners from proto to model
	allowedRunners := make([]models.AllowedRunner, len(req.GetAllowedRunners()))
	for i, ar := range req.GetAllowedRunners() {
		allowedRunners[i] = models.AllowedRunner{
			RunnerID: ar.GetRunnerId(),
			Files:    pbFilesToModel(ar.GetFiles()),
		}
	}

	// Map solution from proto to model
	var solution *models.Solution
	if req.GetSolution() != nil {
		solution = &models.Solution{
			RunnerID: req.GetSolution().GetRunnerId(),
			Files:    pbFilesToModel(req.GetSolution().GetFiles()),
		}
	}

	// Map resource files from proto to model
	resourceFiles := pbFilesToModel(req.GetResourceFiles())

	updatedFields := mongodb.GetUpdatedFields(&models.UpdateTask{
		AllowedRunners: allowedRunners,
		CompareID:      req.CompareScriptId,
		Solution:       solution,
		ResourceFiles:  resourceFiles,
	})

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
		updatedFields["limit"] = limit
	}

	var testcaseGroups []models.TestCaseGroup
	if req.GetTestCaseGroups() != nil {
		var wg errgroup.Group
		var mu sync.Mutex
		for _, group := range req.GetTestCaseGroups() {
			wg.Go(func() error {
				testcaseGroup := models.TestCaseGroup{
					ID:        group.GetId(),
					Name:      group.GetName(),
					Score:     group.GetScore(),
					Order:     group.GetOrder(),
					TestCases: make([]models.TestCase, len(group.GetTestCases())),
				}

				testcases := praseTestCasesPBToModel(group.GetTestCases())

				childCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				defer cancel()

				updateTestCases, err := g.generateTestCases(childCtx, generateTestCasesPayload{
					solution:      solution,
					resourceFiles: resourceFiles,
					testcases:     testcases,
					limit:         limit,
				})
				if err != nil {
					return err
				}

				testcaseGroup.TestCases = updateTestCases

				mu.Lock()
				testcaseGroups = append(testcaseGroups, testcaseGroup)
				mu.Unlock()
				return nil
			})
		}

		err := wg.Wait()
		if err != nil {
			g.logger.Errorw("Failed to generate testcases in some groups", "error", err)
			return nil, errors.New("failed to generate testcases in some groups")
		}
	} else {
		testcaseGroups = []models.TestCaseGroup{}
	}

	updatedFields["test_case_groups"] = testcaseGroups

	_, err := g.db.Collection("tasks").UpdateByID(ctx, req.GetId(), bson.D{{Key: "$set", Value: updatedFields}})
	if err != nil {
		g.logger.Errorw("Failed to upsert task", "error", err, "taskId", req.GetId())
		return nil, status.Errorf(codes.Internal, "failed to upsert task: %v", err)
	}

	err = g.taskCache.InvalidateAll(ctx)
	if err != nil {
		return nil, err
	}

	g.logger.Infow("Task updated", "taskId", req.GetId())

	return nil, nil
}

func praseTestCasesPBToModel(testcasesPB []*pb.TestCase) []models.TestCase {
	testcases := make([]models.TestCase, len(testcasesPB))
	for i, testcasePB := range testcasesPB {
		testcases[i] = models.TestCase{
			ID:       testcasePB.GetId(),
			Order:    testcasePB.GetOrder(),
			Input:    testcasePB.GetInput(),
			Output:   testcasePB.GetOutput(),
			IsHidden: testcasePB.GetIsHidden(),
		}
	}
	return testcases
}

type generateTestCasesPayload struct {
	solution      *models.Solution
	resourceFiles []models.File
	testcases     []models.TestCase
	limit         *models.Limit
}

func (g *grpcServer) generateTestCases(ctx context.Context, payload generateTestCasesPayload) ([]models.TestCase, error) {
	if len(payload.testcases) == 0 {
		return []models.TestCase{}, nil
	}

	if payload.solution == nil || len(payload.solution.Files) == 0 {
		return nil, status.Error(codes.InvalidArgument, "solution files are required to generate test cases")
	}

	if payload.solution.RunnerID == "" {
		return nil, status.Error(codes.InvalidArgument, "solution runner is required to generate test cases")
	}

	var graderLimit *graderPB.Limit
	if payload.limit != nil {
		graderLimit = &graderPB.Limit{
			CpuTime:      payload.limit.CpuTime,
			CpuExtraTime: payload.limit.CpuExtraTime,
			WallTime:     payload.limit.WallTime,
			Memory:       payload.limit.Memory,
			Stack:        payload.limit.Stack,
			MaxOpenFiles: payload.limit.MaxOpenFiles,
			MaxFileSize:  payload.limit.MaxFileSize,
			NetworkAllow: payload.limit.NetworkAllow,
		}
	}

	graderTestcases := make([]*graderPB.TestCaseRequest, len(payload.testcases))
	for i, testcase := range payload.testcases {
		graderTestcases[i] = &graderPB.TestCaseRequest{
			Id:    testcase.ID,
			Order: testcase.Order,
			Input: testcase.Input,
		}
	}

	graderFiles := make([]*graderPB.File, 0, len(payload.solution.Files)+len(payload.resourceFiles))
	for _, file := range payload.solution.Files {
		graderFiles = append(graderFiles, &graderPB.File{
			Name:    file.Name,
			Content: file.Content,
		})
	}
	for _, file := range payload.resourceFiles {
		graderFiles = append(graderFiles, &graderPB.File{
			Name:    file.Name,
			Content: file.Content,
		})
	}

	res, err := g.graderClient.GenerateTestCases(ctx, &graderPB.GenerateTestCasesRequest{
		Files:     graderFiles,
		Testcases: graderTestcases,
		RunnerId:  payload.solution.RunnerID,
		Limit:     graderLimit,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to generate test cases: %v", err)
	}

	updateTestCases := make([]models.TestCase, 0, len(res.GetResults()))
	for _, gt := range res.GetResults() {
		updateTestCases = append(updateTestCases, models.TestCase{
			ID:     gt.GetId(),
			Order:  gt.GetOrder(),
			Input:  gt.GetInput(),
			Output: gt.GetOutput(),
		})
	}

	return updateTestCases, nil
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

	err = g.taskCache.InvalidateAll(ctx)
	if err != nil {
		return nil, err
	}

	g.logger.Infow("Task deleted", "taskId", taskID)
	return &emptypb.Empty{}, nil
}

func (g *grpcServer) RemoveRunnerOnCascade(ctx context.Context, req *pb.RemoveRunnerOnCascadeRequest) (*emptypb.Empty, error) {
	runnerID := req.GetRunnerId()
	if runnerID == "" {
		return nil, status.Error(codes.InvalidArgument, "runner id is required")
	}

	_, err := g.db.Collection("tasks").UpdateMany(ctx, bson.M{
		"allowed_runners.runner_id": runnerID,
	}, bson.M{
		"$pull": bson.M{
			"allowed_runners": bson.M{"runner_id": runnerID},
		},
	})
	if err != nil {
		g.logger.Errorw("Failed to remove allowed runner from tasks", "error", err, "runnerId", runnerID)
		return nil, status.Errorf(codes.Internal, "failed to remove runner from tasks: %v", err)
	}

	_, err = g.db.Collection("tasks").UpdateMany(ctx, bson.M{
		"solution.runner_id": runnerID,
	}, bson.M{
		"$set": bson.M{
			"solution": nil,
		},
	},
	)
	if err != nil {
		g.logger.Errorw("Failed to remove solution runner from tasks", "error", err, "runnerId", runnerID)
		return nil, status.Errorf(codes.Internal, "failed to remove runner from tasks: %v", err)
	}

	err = g.taskCache.InvalidateAll(ctx)
	if err != nil {
		return nil, err
	}

	g.logger.Infow("Removed runner from tasks", "runnerId", runnerID)
	return &emptypb.Empty{}, nil
}

func (g *grpcServer) RemoveCompareScriptOnCascade(ctx context.Context, req *pb.RemoveCompareScriptOnCascadeRequest) (*emptypb.Empty, error) {
	scriptID := req.GetCompareScriptId()
	if scriptID == "" {
		return nil, status.Error(codes.InvalidArgument, "script id is required")
	}

	_, err := g.db.Collection("tasks").UpdateMany(ctx, bson.M{
		"compare_id": scriptID,
	}, bson.M{
		"$set": bson.M{
			"compare_id": nil,
		},
	})
	if err != nil {
		g.logger.Errorw("Failed to remove compare script from tasks", "error", err, "scriptId", scriptID)
		return nil, status.Errorf(codes.Internal, "failed to remove compare script from tasks: %v", err)
	}

	err = g.taskCache.InvalidateAll(ctx)
	if err != nil {
		return nil, err
	}

	g.logger.Infow("Removed compare script from tasks", "scriptId", scriptID)
	return &emptypb.Empty{}, nil
}

// pbFilesToModel converts a slice of proto File messages to model File structs.
func pbFilesToModel(files []*pb.File) []models.File {
	result := make([]models.File, len(files))
	for i, f := range files {
		result[i] = models.File{
			Name:    f.GetName(),
			Content: f.GetContent(),
		}
	}
	return result
}

// modelFilesToPB converts a slice of model File structs to proto File messages.
func modelFilesToPB(files []models.File) []*pb.File {
	result := make([]*pb.File, len(files))
	for i, f := range files {
		result[i] = &pb.File{
			Name:    f.Name,
			Content: f.Content,
		}
	}
	return result
}

// taskModelToPB converts a Task model to a proto TaskResponse.
func taskModelToPB(task *models.Task) *pb.TaskResponse {
	var pbLimit *pb.Limit
	if task.Limit != nil {
		pbLimit = &pb.Limit{
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

	var pbSolution *pb.Solution
	if task.Solution != nil {
		pbSolution = &pb.Solution{
			RunnerId: task.Solution.RunnerID,
			Files:    modelFilesToPB(task.Solution.Files),
		}
	}

	allowedRunners := make([]*pb.AllowedRunner, len(task.AllowedRunners))
	for i, ar := range task.AllowedRunners {
		allowedRunners[i] = &pb.AllowedRunner{
			RunnerId: ar.RunnerID,
			Files:    modelFilesToPB(ar.Files),
		}
	}

	testCaseGroups := make([]*pb.TestCaseGroup, len(task.TestCaseGroups))
	for i, g := range task.TestCaseGroups {
		testCases := make([]*pb.TestCase, len(g.TestCases))
		for j, tc := range g.TestCases {
			testCases[j] = &pb.TestCase{
				Id:       tc.ID,
				Order:    tc.Order,
				Input:    tc.Input,
				Output:   tc.Output,
				IsHidden: tc.IsHidden,
			}
		}
		testCaseGroups[i] = &pb.TestCaseGroup{
			Id:        g.ID,
			Name:      g.Name,
			Score:     g.Score,
			Order:     g.Order,
			TestCases: testCases,
		}
	}

	return &pb.TaskResponse{
		Id:              task.ID,
		AllowedRunners:  allowedRunners,
		CompareScriptId: task.CompareID,
		Limit:           pbLimit,
		Solution:        pbSolution,
		TestCaseGroups:  testCaseGroups,
		ResourceFiles:   modelFilesToPB(task.ResourceFiles),
	}
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
