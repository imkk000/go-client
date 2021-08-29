package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	_ "embed"
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

//go:embed cert/ca-cert.pem
var awsRootCAPem []byte

const (
	maxAuthTokenRetries = 2
)

type MysqlIAM struct {
	OverridePassword bool
	AWSRegion        string
	AWSCredentials   aws.CredentialsProvider
}

func (m *MysqlIAM) Open(dsn string) (driver.Conn, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	// override password
	if m.OverridePassword {
		var authToken string
		var err error
		for i := 0; i < maxAuthTokenRetries; i++ {
			authToken, err = auth.BuildAuthToken(context.Background(), cfg.Addr, m.AWSRegion, cfg.User, m.AWSCredentials)
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil, err
		}
		cfg.Passwd = authToken
	}

	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Fatal("recovery:", err)
		}
	}()

	// load env
	dbEndpoint := os.Getenv("DB_HOSTNAME")
	dbUser := os.Getenv("DB_USERNAME")
	dbName := os.Getenv("DB_NAME")
	fmt.Println("load env:", dbEndpoint, dbUser, dbName)

	// load aws config
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal("load config:", err)
	}
	fmt.Println(cfg.Region, cfg.Credentials, cfg.EndpointResolver)

	// register tls ca
	rootCertPool := x509.NewCertPool()
	if ok := rootCertPool.AppendCertsFromPEM(awsRootCAPem); !ok {
		log.Fatal("append PEM")
	}
	mysql.RegisterTLSConfig("aws-rds", &tls.Config{
		RootCAs: rootCertPool,
	})
	sql.Register("mysql-aws-iam", &MysqlIAM{
		OverridePassword: true,
		AWSRegion:        cfg.Region,
		AWSCredentials:   cfg.Credentials,
	})

	mysqlConfig := mysql.Config{
		User:                    dbUser,
		Passwd:                  "#OVERRIDE#",
		Net:                     "tcp",
		Addr:                    dbEndpoint,
		DBName:                  dbName,
		Collation:               "utf8mb4_general_ci",
		Loc:                     time.UTC,
		MaxAllowedPacket:        4 << 20,
		TLSConfig:               "aws-rds",
		Timeout:                 10 * time.Second,
		ReadTimeout:             10 * time.Second,
		WriteTimeout:            10 * time.Second,
		AllowCleartextPasswords: true,
		AllowNativePasswords:    true,
		CheckConnLiveness:       true,
		ParseTime:               true,
	}
	connectStr := mysqlConfig.FormatDSN()
	log.Println("connection string:", connectStr)
	// Default
	db, err := sql.Open("mysql-aws-iam", connectStr)
	if err != nil {
		log.Fatal("create dsn:", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Fatal("close db connection:", err)
		}
	}()
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(15)
	db.SetConnMaxLifetime(10 * time.Minute)
	if err := db.Ping(); err != nil {
		log.Println("ping typeof:", reflect.TypeOf(err))
		log.Println("try to ping:", err)
	}

	rows, err := db.Query(`SELECT * FROM test`)
	if err != nil {
		log.Println("query rows:", err)
	}

	var id sql.NullInt32
	var name sql.NullString
	var createdAt, updatedAt sql.NullTime
	for rows.Next() {
		if err := rows.Scan(&id, &name, &createdAt, &updatedAt); err != nil {
			log.Println("scan:", err)
			continue
		}
		fmt.Println("mysql:", id, name, createdAt, updatedAt)
	}

	// SQLX
	xdb, err := sqlx.Open("mysql-aws-iam", connectStr)
	if err != nil {
		log.Fatal("create dsn sqlx:", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Fatal("close xdb connection:", err)
		}
	}()
	if err := db.Ping(); err != nil {
		log.Fatal("try to ping sqlx:", err)
	}

	xrows, err := xdb.Queryx(`SELECT * FROM test`)
	if err != nil {
		log.Fatal("query rows sqlx:", err)
	}
	fmt.Println(xrows.Columns())
	ct, _ := xrows.ColumnTypes()
	for _, c := range ct {
		fmt.Println(c.Name(), c.DatabaseTypeName())
		fmt.Println(c.DecimalSize())
		fmt.Println(c.Length())
		fmt.Println(c.Nullable())
		fmt.Println(c.ScanType())
	}
	var data struct {
		ID        int       `db:"id"`
		Name      *string   `db:"name"`
		CreatedAt time.Time `db:"created_at"`
		UpdatedAt time.Time `db:"updated_at"`
	}
	for xrows.Next() {
		if err := xrows.StructScan(&data); err != nil {
			log.Println("sqlx scan:", err)
			continue
		}
		fmt.Println("sqlx:", data)
	}
}
