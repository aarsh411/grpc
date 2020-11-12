package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/grpc/proto"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
)

//InputData Struct
type InputData struct {
	Username string `json:"username"`
	Name     string `json:"name"`
}

func main() {
	conn, err := grpc.Dial("localhost:4040", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	client := proto.NewAddServiceClient(conn)

	g := gin.Default()
	g.POST("/add", func(ctx *gin.Context) {

		var inputData InputData
		ctx.BindJSON(&inputData)
		username := inputData.Username

		name := inputData.Name

		req := &proto.Request{Username: string(username), Name: string(name)}
		if response, err := client.Add(ctx, req); err == nil {
			ctx.JSON(http.StatusOK, gin.H{
				"result": fmt.Sprint(response.Result),
			})
		} else {
			ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
	})

	if err := g.Run(":8080"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}

}
