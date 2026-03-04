package main

import (
	"fmt"

	"github.com/miretskiy/firn/polars"
)

func main() {
	fmt.Println("Start")

	// df_s3_1, err := polars.ReadParquet("s3://test/1.parquet").Limit(1).Collect()

	// fmt.Println(df_s3_1.JSON())

	df, err := polars.NewDataFrame(
		polars.NewSeries("name", []string{"Alice", "Bob", "Charlie"}),
		polars.NewSeries("age", []int{25, 30, 35}),
		polars.NewSeries("salary", []float64{50000, 60000, 70000}),
	).Collect()

	if err != nil {
		panic(err)
	}
	fmt.Println(df.String())

	data := map[string]interface{}{
		"product": []string{"A", "B", "C", "A", "B"},
		"price":   []float64{10.0, 15.0, 20.0, 12.0, 18.0},
		"qty":     []int{100, 200, 150, 80, 120},
	}

	df1, err := polars.FromMap(data).Collect()

	if err != nil {
		panic(err)
	}
	fmt.Println(df1.String())

	// s := time.Now()

	/*
		Не забудь задать переменные окружения
		export AWS_ACCESS_KEY_ID=adminminio
		export AWS_SECRET_ACCESS_KEY=adminminio
		export AWS_ENDPOINT_URL=http://localhost:32020
		export AWS_REGION=us-east-1
		export AWS_BUCKET_NAME=test
	*/
	// df_s3r_1, err := polars.ReadParquet("s3://test/*.parquet").Select("event_id", "ATM_ACQUIRING_ICC", "ATM_MERCHANT_ID", "user_profile_num_of_payments", "device_source_sdk", "USER_BIRTHDATE", "USER_ID", "USER_FIRST_NAME", "ext_map").Collect()
	// if err != nil {
	// 	panic(err)
	// }
	// df_s3r_2, err := polars.ReadParquet("s3://test/2.parquet").Select("event_id", "ATM_ACQUIRING_ICC", "ATM_MERCHANT_ID", "user_profile_num_of_payments", "device_source_sdk", "USER_BIRTHDATE", "USER_ID", "USER_FIRST_NAME", "ext_map").Collect()
	// if err != nil {
	// 	panic(err)
	// }
	// df_s3r_3, err := polars.ReadParquet("s3://test/3.parquet").Select("event_id", "ATM_ACQUIRING_ICC", "ATM_MERCHANT_ID", "user_profile_num_of_payments", "device_source_sdk", "USER_BIRTHDATE", "USER_ID", "USER_FIRST_NAME", "ext_map").Collect()
	// if err != nil {
	// 	panic(err)
	// }
	// df_s3r_4, err := polars.ReadParquet("s3://test/4.parquet").Select("event_id", "ATM_ACQUIRING_ICC", "ATM_MERCHANT_ID", "user_profile_num_of_payments", "device_source_sdk", "USER_BIRTHDATE", "USER_ID", "USER_FIRST_NAME", "ext_map").Collect()
	// if err != nil {
	// 	panic(err)
	// }
	// df_all, err := polars.Concat(df_s3r_1, df_s3r_2, df_s3r_3, df_s3r_4).Collect()

	// // df_s3, err := df_s3r_1.Select("event_id", "ATM_ACQUIRING_ICC", "user_profile_num_of_payments", "USER_BIRTHDATE", "ext_map").Limit(50000).Collect()

	// if err != nil {
	// 	panic(err)
	// }

	// e := time.Now()

	// fmt.Println(df_all)
	// fmt.Println("чтение без фильтра лимит строк и колонок:", e.Sub(s).Milliseconds(), "мс")

	// s = time.Now()
	// concated := polars.Concat(df_s3r_1, df_s3r_2, df_s3r_3, df_s3r_4)
	// df_agg, err := concated.Select("USER_FIRST_NAME").GroupBy("USER_FIRST_NAME").Agg("count(*) as cnt").SortBy([]polars.SortField{{Column: "cnt", Direction: polars.Descending}}).Collect()
	// e = time.Now()

	// fmt.Println(df_agg)
	// fmt.Println("чтение с группировкой и агрегацией:", e.Sub(s).Milliseconds(), "мс")

	// s = time.Now()
	// df, err := polars.ReadParquetWithOptions("s3://test/*.parquet", polars.ParquetOptions{WithGlob: true}).Select("event_id", "ATM_ACQUIRING_ICC", "ATM_MERCHANT_ID", "user_profile_num_of_payments", "device_source_sdk", "USER_BIRTHDATE", "USER_ID", "USER_FIRST_NAME", "ext_map").Limit(50000).Collect()
	// if err != nil {
	// 	panic(err)
	// }
	// e = time.Now()
	// fmt.Println(df)
	// fmt.Println("чтение bulk:", e.Sub(s).Milliseconds(), "мс")

	// s = time.Now()
	// df_c := polars.ReadParquetWithOptions("s3://test/*.parquet", polars.ParquetOptions{WithGlob: true})
	// df_agg_c, err := df_c.Select("USER_FIRST_NAME").GroupBy("USER_FIRST_NAME").Agg("count(*) as cnt").SortBy([]polars.SortField{{Column: "cnt", Direction: polars.Descending}}).Collect()
	// e = time.Now()

	// fmt.Println(df_agg_c.JSON())
	// fmt.Println("чтение bulk с группировкой и агрегацией:", e.Sub(s).Milliseconds(), "мс")

	fmt.Println("End")

}
