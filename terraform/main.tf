provider "aws" {
  region = "eu-central-1"
}

# Create the IAM user
resource "aws_iam_user" "my_iam_user" {
  name = "scigility"
}

# Enable Console Access (Login Profile) without setting password directly
resource "aws_iam_user_login_profile" "scigility_login" {
  user                     = aws_iam_user.my_iam_user.name
  password_reset_required  = true  # Force the user to change the password at first login
}

# Create Access Keys for Programmatic Access
resource "aws_iam_access_key" "scigility_access_key" {
  user = aws_iam_user.my_iam_user.name
}

# Attach the AmazonS3FullAccess managed policy to the user
resource "aws_iam_user_policy_attachment" "my_user_policy_attachment" {
  user       = aws_iam_user.my_iam_user.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# Create the S3 bucket
resource "aws_s3_bucket" "my_s3_bucket" {
  bucket = "nmourmx-scigility"  # Make sure the bucket name is globally unique

  tags = {
    Name        = "nmourmx-scigility"
    Environment = "dev"
  }
}

# Enable versioning for the S3 bucket
resource "aws_s3_bucket_versioning" "versioning_example" {
  bucket = aws_s3_bucket.my_s3_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}


# --- ECR Repository (Private by default) ---

resource "aws_ecr_repository" "scigility_repo" {
  name                  = "scigility-ecr-repo"
  image_tag_mutability  = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name        = "scigility-ecr-repo"
    Environment = "dev"
  }
}