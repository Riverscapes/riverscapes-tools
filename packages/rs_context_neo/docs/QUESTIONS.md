
## Configuration bloat

- What do we do for non US cases? Is rs_context_neo just for US cases? Do we need a whole new tool for each country?

What about a configuration file that specifies which layers our tool should have and where they should come from?

Other countries won't have landfire or may have various other ways to specify where their DEMs come from. Some countries may want precipitation. Others might not. I'm really struggling with exposing all these as POSIX arguments on the command line. 

I need a new idea.

## Permissions

- Cybercastor is going to need direct access to the S3 bucket where the COGS are stored. This is because it needs to be able to read the COGS in order to serve them to users, and it also needs to be able to write to the bucket in order to store any new COGS that are generated.
- Cybercastor is also going to need direct read access to Athena to read raster data


## COGS on S3

Where do we keep them? Is there a process for putting them there? Are we namespacing this at all?

We can now mount the Warehouse bucket directly as a read-only file system in Fargate using the new Amazon S3 Files feature. This allows us to read COGS directly from S3 without needing to download them locally or use a sidecar container.

(Note: this does expose some pretty problematic security implications since every cybercastor instance would have read access to every project in the bucket. Maybe for starters we just have a shared bucket with namespaced areas for each cybercastor engine and put the national project there)


```
/ENGINE_NAME/TASKNAME/COGS/COG_NAME.tif
```

## Caching on S3

EFS is live so there's file locking problems. If we're going to use S3 Files for cahcing we need to change the behaviour slightly

- Auto lifecycle events mean we can clean up cache more effectively
- processes can use the same cache but they MAY not need the same file locking workflows. If you get a cache hit then use it. If you get a miss then download the file, use it then write it to the cache. Much simpler in theory. Does it work in practice.?

------------------

From Gemini:

AWS introduced **Amazon S3 Files**, which allows you to expose any general-purpose S3 bucket (or scoped prefix) as a fully featured, POSIX-compliant shared file system.

Historically, mounting S3 into AWS Fargate required clunky workarounds like running `rclone` or `goofys` in a sidecar container, or downloading data locally on startup. With this native integration, you can mount S3 buckets directly onto your Fargate containers using standard ECS Task Definition configurations.

---

## How It Works Under the Hood

S3 Files bridges the gap between object storage and file storage by linking your bucket to a managed file system structure built on NFS technology.

* **Caching & Performance:** Active files and metadata are automatically cached on high-performance infrastructure to provide sub-millisecond latencies for your app.
* **Direct Streaming:** For large sequential reads ($\ge$ 1 MiB), it streams data straight from S3 to maximize throughput.
* **Write-Back Caching:** When your application writes data, it lands in the cache first and asynchronously batch-syncs to S3 (typically taking around 60 seconds to become visible in your bucket).

> 💡 **Fun Fact:** S3 Files shares its network layer with Amazon EFS. Because of this, the IAM service role for S3 Files actually uses the `elasticfilesystem.amazonaws.com` service principal instead of a unique S3 string!

---

## Step-by-Step Setup Guide

To use this feature, you must configure your infrastructure, update your Fargate Task's IAM role, and define the volume in your task definition.

### 1. Set Up the S3 Files Infrastructure

Before updating ECS, you must provision the file system construct inside your VPC:

1. Create an **S3 Files File System** via the AWS Console or CLI and link it to your target S3 bucket.
2. Create **Mount Targets** in the same VPC subnets where your Fargate tasks run. Ensure your Fargate Security Group allows outbound NFS traffic (TCP port 2049) to the mount targets.
3. (Recommended) Create an **S3 Files Access Point** to enforce a POSIX user identity and isolate the container's root directory.

### 2. Update the Fargate Task IAM Role

Your **Task Role** (the role that your application code uses, *not* the Task Execution Role) needs explicit permission to interact with both the S3 Files file system and the underlying S3 bucket. Attach the following policy to your Task Role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Side": "S3FilesAccess",
      "Effect": "Allow",
      "Action": [
        "s3files:ClientMount",
        "s3files:ClientWrite"
      ],
      "Resource": "arn:aws:s3files:us-east-1:123456789012:file-system/fs-12345678"
    },
    {
      "Side": "S3BucketAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-target-bucket-name",
        "arn:aws:s3:::your-target-bucket-name/*"
      ]
    }
  ]
}

```

### 3. Configure the ECS Task Definition

Now, declare the volume using the `s3FilesVolumeConfiguration` block in your Task Definition JSON and point your container to it using `mountPoints`.

Here is a minimal Fargate-compliant task definition template:

```json
{
  "family": "s3-fargate-task",
  "networkMode": "awsvpc",
  "requiresCompatibilities": [
    "FARGATE"
  ],
  "cpu": "512",
  "memory": "1024",
  "taskRoleArn": "arn:aws:iam::123456789012:role/YourTaskRoleName",
  "executionRoleArn": "arn:aws:iam::123456789012:role/YourExecutionRoleName",
  "containerDefinitions": [
    {
      "name": "app-container",
      "image": "nginx:latest",
      "essential": true,
      "mountPoints": [
        {
          "sourceVolume": "s3-storage",
          "containerPath": "/mnt/s3data",
          "readOnly": false
        }
      ]
    }
  ],
  "volumes": [
    {
      "name": "s3-storage",
      "s3FilesVolumeConfiguration": {
        "fileSystemArn": "arn:aws:s3files:us-east-1:123456789012:file-system/fs-12345678",
        "accessPointArn": "arn:aws:s3files:us-east-1:123456789012:access-point/fsap-12345678",
        "rootDirectory": "/"
      }
    }
  ]
}

```

---

## Important Gotchas to Keep in Mind

* **POSIX Limitations:** While S3 Files supports standard file operations (`open`, `read`, `write`, `seek`), it does not support more complex OS-level requirements like file locking, hard links, or renaming open files. Avoid using this as storage for active relational databases.
* **Network Paths:** Because all communication stays within AWS using the NFS layer, it is highly recommended to configure an **S3 Gateway VPC Endpoint** in your VPC routing tables. This prevents your S3 file traffic from routing out over the public internet, optimizing performance and avoiding data transfer fees.