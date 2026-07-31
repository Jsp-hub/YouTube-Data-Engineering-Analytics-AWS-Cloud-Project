#Lambda Permissions:

{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowListBuckets",
			"Effect": "Allow",
			"Action": [
				"s3:ListBucket"
			],
			"Resource": [
				"arn:aws:s3:::yt-data-pipeline-bronze-us-east-1-devjeel",
				"arn:aws:s3:::yt-data-pipeline-silver-us-east-1-devjeel"
			]
		},
		{
			"Sid": "AllowReadWriteObjects",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:PutObject",
				"s3:DeleteObject"
			],
			"Resource": [
				"arn:aws:s3:::yt-data-pipeline-bronze-us-east-1-devjeel/*",
				"arn:aws:s3:::yt-data-pipeline-silver-us-east-1-devjeel/*"
			]
		},
		{
			"Sid": "GlueAccess",
			"Effect": "Allow",
			"Action": [
				"glue:GetTable",
				"glue:GetDatabase",
				"glue:CreateTable",
				"glue:UpdateTable",
				"glue:GetPartitions",
				"glue:CreatePartition",
				"glue:BatchCreatePartition"
			],
			"Resource": "*"
		},
		{
			"Sid": "SNSAccess",
			"Effect": "Allow",
			"Action": [
				"sns:Publish"
			],
			"Resource": "arn:aws:sns:us-east-1:<AWS Account>:Pipeline-Alerts"
		},
		{
			"Sid": "AthenaAccess",
			"Effect": "Allow",
			"Action": [
				"athena:StartQueryExecution",
				"athena:GetQueryExecution",
				"athena:GetQueryResults",
				"athena:StopQueryExecution",
				"athena:GetWorkGroup"
			],
			"Resource": "*"
		},
		{
		    "Sid": "AthenaQueryResultsAccess",
		    "Effect": "Allow",
		    "Action": [
		        "s3:GetBucketLocation",
		        "s3:ListBucket"
		    ],
		    "Resource": [
		        "arn:aws:s3:::query-result-bucket-using-athena"
		    ]
		},
		{
		    "Sid": "AthenaQueryResultsObjectAccess",
		    "Effect": "Allow",
		    "Action": [
		        "s3:GetObject",
		        "s3:PutObject",
		        "s3:DeleteObject"
		    ],
		    "Resource": [
		        "arn:aws:s3:::query-result-bucket-using-athena/*"
		    ]
		}
	]
}



Glue Permissions:

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3FullAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::yt-data-pipeline-bronze-us-east-1-devjeel/*",
                "arn:aws:s3:::yt-data-pipeline-bronze-us-east-1-devjeel ",
                "arn:aws:s3:::yt-data-pipeline-silver-us-east-1-devjeel/*",
                "arn:aws:s3:::yt-data-pipeline-silver-us-east-1-devjeel ",
                "arn:aws:s3:::yt-data-pipeline-gold-us-east-1-devjeel/*",
                "arn:aws:s3:::yt-data-pipeline-gold-us-east-1-devjeel ",
                "arn:aws:s3:::yt-data-pipeline-script-us-east-1-devjeel/*",
                "arn:aws:s3:::yt-data-pipeline-script-us-east-1-devjeel "
            ]
        }
    ]
}


Step Function Permissions:
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": "lambda:InvokeFunction",
			"Resource": "*"
		},
		{
			"Effect": "Allow",
			"Action": [
				"glue:StartJobRun",
				"glue:GetJobRun",
				"glue:GetJobRuns",
				"glue:BatchStopJobRun"
			],
			"Resource": "*"
		},
		{
			"Effect": "Allow",
			"Action": "sns:Publish",
			"Resource": "arn:aws:sns:us-east-1:<AWS Account>:Pipeline-Alerts"
		}
	]
}


yt-api-data-ingestion-function-role

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "logs:CreateLogGroup",
            "Resource": "arn:aws:logs:us-east-1:<AWS Account>:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:us-east-1:<AWS Account>:log-group:/aws/lambda/yt-api-data-ingestion-function:*"
            ]
        }
    ]
}