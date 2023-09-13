# Lambda Function for work project:

Handling processing assets in the background to avoid timeouts on serverless monorepo and planetscale

## To run on Lambda:

```bash
GOOS=linux GOARCH=amd64 go build -o main main.go
```

The above will create a linux binary called main

```bash
zip function.zip main
```

The above will zip the binary into a zip file called function.zip

Upload to AWS Using the GUI or Command line
