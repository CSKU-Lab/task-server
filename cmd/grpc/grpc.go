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

		tasks = append(tasks, &pb.TaskResponse{
			Id:        task.ID,
			RunnerId:  task.RunnerID,
			CompareId: task.CompareID,
			Solution:  task.Solution,
			Testcases: func() []*pb.TestCaseResponse {
				var testcases []*pb.TestCaseResponse
				for _, testcase := range task.TestCases {
					testcases = append(testcases, &pb.TestCaseResponse{
						Id:     testcase.ID,
						Input:  testcase.Input,
						Output: testcase.Output,
					})
				}
				return testcases
			}(),
		})
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
		Id:        task.ID,
		RunnerId:  task.RunnerID,
		CompareId: task.CompareID,
		Solution:  task.Solution,
		Testcases: func() []*pb.TestCaseResponse {
			var testcases []*pb.TestCaseResponse
			for _, testcase := range task.TestCases {
				testcases = append(testcases, &pb.TestCaseResponse{
					Id:     testcase.ID,
					Input:  testcase.Input,
					Output: testcase.Output,
				})
			}
			return testcases
		}(),
	}, nil
}

func (g *grpcServer) SetTask(ctx context.Context, req *pb.SetTaskRequest) (*pb.SetTaskResponse, error) {
	id := req.GetId()
	if id == "" {
		uuid, err := uuid.NewV7()
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to generate UUID: %v", err)
		}
		id = uuid.String()
	}

	var err error

	updatedFields := mongodb.GetUpdatedFields(&models.UpdateTask{
		RunnerID:  req.RunnerId,
		CompareID: req.CompareId,
		Solution:  req.Solution,
		TestCases: func() *[]models.TestCase {
			if len(req.GetTestcases()) == 0 {
				return nil
			}

			var testcases []models.TestCase
			for _, testcase := range req.GetTestcases() {
				id, _err := uuid.NewV7()
				if _err != nil {
					err = _err
				}

				testcases = append(testcases, models.TestCase{
					ID:     id.String(),
					Input:  testcase.GetInput(),
					Output: testcase.GetOutput(),
				})
			}
			return &testcases
		}(),
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to process test cases: %v", err)
	}

	opts := options.UpdateOne().SetUpsert(true)

	_, err = g.db.Collection("tasks").UpdateByID(ctx, id, bson.D{{Key: "$set", Value: updatedFields}}, opts)
	if err != nil {
		return nil, err
	}

	return &pb.SetTaskResponse{
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
