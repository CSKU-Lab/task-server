package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/CSKU-Lab/task-service/configs"
	pb "github.com/CSKU-Lab/task-service/genproto/task/v1"
	"github.com/CSKU-Lab/task-service/models"
	"github.com/CSKU-Lab/task-service/mongodb"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func main() {
	env := configs.NewEnv()

	client, err := mongo.Connect(options.Client().ApplyURI(env.GetMongoURI()))
	if err != nil {
		log.Fatalln(err)
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		log.Fatalln("Failed to ping MongoDB: ", err)
	}

	db := client.Database("db")

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", env.GetPort()))
	if err != nil {
		log.Fatalln("failed to listen: ", err)
	}

	s := grpc.NewServer()
	pb.RegisterTaskServiceServer(s, &grpcServer{
		db: db,
	})
	reflection.Register(s)
	log.Println("gRPC ConfigService registered")

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		sig := <-sigs
		log.Printf("Receive %s signal from OS, going to shutdown...\n", sig)
		timer := time.AfterFunc(10*time.Second, func() {
			log.Println("Server couldn't stop grafully in time. Doing force stop.")
		})
		defer timer.Stop()

		s.GracefulStop()
	}()

	if err := s.Serve(lis); err != nil {
		log.Fatalln("Cannot start grpc server :", err)
	}
}

type grpcServer struct {
	pb.UnimplementedTaskServiceServer
	db *mongo.Database
}

func NewGRPCServer(db *mongo.Database) pb.TaskServiceServer {
	return &grpcServer{
		db: db,
	}
}

// this method is only use for postman maybe no need to use in production
// because in main service we already pagination there and just receive task_id from there and query just only that ids is enough
func (g *grpcServer) GetTasks(ctx context.Context, req *pb.GetTasksRequest) (*pb.GetTasksResponse, error) {
	cursor, err := g.db.Collection("tasks").Find(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to find tasks: %v", err)
	}
	defer cursor.Close(ctx)

	var tasks []*pb.TaskResponse
	for cursor.Next(ctx) {
		var task models.Task
		if err := cursor.Decode(&task); err != nil {
			return nil, fmt.Errorf("failed to decode task: %v", err)
		}

		taskRes := &pb.TaskResponse{
			Id:               task.ID,
			Solution:         task.Solution,
			AllowedRunnerIds: task.AllowedRunnerIDs,
			CompareScriptId:  task.CompareID,
		}

		var testcases []*pb.TestCase
		for _, testcase := range task.TestCases {
			testcases = append(testcases, &pb.TestCase{
				Order:  testcase.Order,
				Input:  testcase.Input,
				Output: testcase.Output,
			})
		}

		taskRes.Testcases = testcases

		tasks = append(tasks, taskRes)
	}

	return &pb.GetTasksResponse{Tasks: tasks}, nil
}

func (g *grpcServer) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.TaskResponse, error) {
	taskID := req.GetId()
	if taskID == "" {
		return nil, fmt.Errorf("task id is required")
	}

	var task models.Task
	err := g.db.Collection("tasks").FindOne(ctx, bson.M{"_id": taskID}).Decode(&task)
	if err != nil {
		return nil, fmt.Errorf("failed to find task: %v", err)
	}

	return &pb.TaskResponse{
		Id:               task.ID,
		Solution:         task.Solution,
		AllowedRunnerIds: task.AllowedRunnerIDs,
		CompareScriptId:  task.CompareID,
		Testcases: func() []*pb.TestCase {
			var testcases []*pb.TestCase
			for _, testcase := range task.TestCases {
				testcases = append(testcases, &pb.TestCase{
					Order:  testcase.Order,
					Input:  testcase.Input,
					Output: testcase.Output,
				})
			}
			return testcases
		}(),
	}, nil
}

func (g *grpcServer) UpsertTask(ctx context.Context, req *pb.UpsertTaskRequest) (*pb.UpsertTaskResponse, error) {
	id := req.GetId()
	if id == "" {
		uuid, err := uuid.NewV7()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to generate UUID: %v", err)
		}
		id = uuid.String()
	}

	allowedRunnerIDs := req.GetAllowedRunnerIds()
	if allowedRunnerIDs == nil {
		allowedRunnerIDs = []string{}
	}

	updatedFields := mongodb.GetUpdatedFields(&models.UpdateTask{
		Solution:         req.Solution,
		AllowedRunnerIDs: allowedRunnerIDs,
		CompareID:        req.CompareId,
		TestCases: func() []models.TestCase {
			if len(req.GetTestcases()) == 0 {
				return []models.TestCase{}
			}

			var testcases []models.TestCase
			for _, testcase := range req.GetTestcases() {
				testcases = append(testcases, models.TestCase{
					Order:  testcase.GetOrder(),
					Input:  testcase.GetInput(),
					Output: testcase.GetOutput(),
				})
			}
			return testcases
		}(),
	})

	opts := options.UpdateOne().SetUpsert(true)

	_, err := g.db.Collection("tasks").UpdateByID(ctx, id, bson.D{{Key: "$set", Value: updatedFields}}, opts)
	if err != nil {
		return nil, err
	}

	return &pb.UpsertTaskResponse{
		Id: id,
	}, nil
}

func (g *grpcServer) DeleteTask(ctx context.Context, req *pb.DeleteTaskRequest) (*emptypb.Empty, error) {
	taskID := req.GetId()
	if taskID == "" {
		return nil, fmt.Errorf("task id is required")
	}

	_, err := g.db.Collection("tasks").DeleteOne(ctx, bson.M{"_id": taskID})
	if err != nil {
		return nil, fmt.Errorf("failed to delete task: %v", err)
	}

	return &emptypb.Empty{}, nil
}
