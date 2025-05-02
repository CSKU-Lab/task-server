package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/CSKU-Lab/task-service/configs"
	pb "github.com/CSKU-Lab/task-service/genproto/task/v1"
	"github.com/CSKU-Lab/task-service/models"
	"github.com/CSKU-Lab/task-service/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", env.GetPort()))
	if err != nil {
		log.Fatalln("failed to listen: ", err)
	}

	s := grpc.NewServer()
	pb.RegisterTaskServiceServer(s, &grpcServer{
		db: db,
	})
	reflection.Register(s)
	log.Println("gRPC ConfigService registered")

	if err := s.Serve(lis); err != nil {
		log.Fatalln("Cannot start grpc server :", err)
	}
}

type grpcServer struct {
	pb.UnimplementedTaskServiceServer
	db *mongo.Database
}

func (g *grpcServer) AddTask(ctx context.Context, req *pb.AddTaskRequest) (*pb.Task, error) {
	if req.GetId() == "" {
		return nil, fmt.Errorf("task id is required")
	}

	task := &models.Task{
		ID:        req.GetId(),
		TestCases: make([]models.TestCase, 0),
	}

	for _, testcase := range req.GetTestcases() {
		task.TestCases = append(task.TestCases, models.TestCase{
			ID:     testcase.GetId(),
			Input:  testcase.GetInput(),
			Output: testcase.GetOutput(),
		})
	}

	_, err := g.db.Collection("tasks").InsertOne(ctx, task)
	if err != nil {
		return nil, fmt.Errorf("failed to insert task: %v", err)
	}

	return &pb.Task{
		Id:        req.GetId(),
		Testcases: req.GetTestcases(),
	}, nil
}

func (g *grpcServer) GetTasks(ctx context.Context, req *pb.GetTasksRequest) (*pb.GetTasksResponse, error) {
	cursor, err := g.db.Collection("tasks").Find(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("failed to find tasks: %v", err)
	}
	defer cursor.Close(ctx)

	var tasks []*pb.Task
	for cursor.Next(ctx) {
		var task models.Task
		if err := cursor.Decode(&task); err != nil {
			return nil, fmt.Errorf("failed to decode task: %v", err)
		}

		tasks = append(tasks, &pb.Task{
			Id: task.ID,
			Testcases: func() []*pb.TestCase {
				var testcases []*pb.TestCase
				for _, testcase := range task.TestCases {
					testcases = append(testcases, &pb.TestCase{
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

func (g *grpcServer) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.Task, error) {
	taskID := req.GetId()
	if taskID == "" {
		return nil, fmt.Errorf("task id is required")
	}

	var task models.Task
	err := g.db.Collection("tasks").FindOne(ctx, bson.M{"_id": taskID}).Decode(&task)
	if err != nil {
		return nil, fmt.Errorf("failed to find task: %v", err)
	}

	return &pb.Task{
		Id: task.ID,
		Testcases: func() []*pb.TestCase {
			var testcases []*pb.TestCase
			for _, testcase := range task.TestCases {
				testcases = append(testcases, &pb.TestCase{
					Id:     testcase.ID,
					Input:  testcase.Input,
					Output: testcase.Output,
				})
			}
			return testcases
		}(),
	}, nil
}

func (g *grpcServer) UpdateTask(ctx context.Context, req *pb.UpdateTaskRequest) (*pb.Task, error) {
	updatedFields := mongodb.GetUpdatedFields(req)

	_, err := g.db.Collection("tasks").UpdateByID(ctx, req.GetId(), updatedFields)
	if err != nil {
		return nil, err
	}
	return &pb.Task{
		Id:       req.GetId(),
		Solution: req.GetSolution(),
		Testcases: func() []*pb.TestCase {
			var testcases []*pb.TestCase
			for _, testcase := range req.GetTestcases() {
				testcases = append(testcases, &pb.TestCase{
					Id:     testcase.GetId(),
					Input:  testcase.GetInput(),
					Output: testcase.GetOutput(),
				})
			}
			return testcases
		}(),
	}, nil
}

func (g *grpcServer) DeleteTask(ctx context.Context, req *pb.DeleteTaskRequest) (*pb.DeleteTaskResponse, error) {
	taskID := req.GetId()
	if taskID == "" {
		return nil, fmt.Errorf("task id is required")
	}

	_, err := g.db.Collection("tasks").DeleteOne(ctx, bson.M{"_id": taskID})
	if err != nil {
		return nil, fmt.Errorf("failed to delete task: %v", err)
	}

	return &pb.DeleteTaskResponse{}, nil
}
