package main

import (
	"fmt"

	"github.com/miretskiy/firn/polars"
)

func main() {
	fmt.Println("TEST")

	/*
		Не забудь задать переменные окружения
		export AWS_ACCESS_KEY_ID=adminminio
		export AWS_SECRET_ACCESS_KEY=adminminio
		export AWS_ENDPOINT_URL=http://localhost:32020
		export AWS_REGION=us-east-1
		export AWS_BUCKET_NAME=test
	*/
	df_s3r := polars.ReadParquet("s3://test/1.parquet")
	df_s3, err := df_s3r.Collect()

	if err != nil {
		fmt.Println(err)
		panic(err)
	}

	fmt.Println(df_s3)

}
