package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	st "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const accountID = "113612240996"

var identifier = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

func awsConfig(ctx context.Context, region string) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return cfg, err
	}
	ident, err := sts.NewFromConfig(cfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return cfg, fmt.Errorf("STS identity check: %w", err)
	}
	if aws.ToString(ident.Account) != accountID {
		return cfg, fmt.Errorf("wrong AWS account %q; expected %q", aws.ToString(ident.Account), accountID)
	}
	log.Printf("AWS account verified: %s", aws.ToString(ident.Account))
	return cfg, nil
}
func versioning(ctx context.Context, c *s3.Client, bucket string) (string, error) {
	out, err := c.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: aws.String(bucket)})
	if err != nil {
		return "", err
	}
	switch out.Status {
	case st.BucketVersioningStatusEnabled:
		return "Enabled", nil
	case st.BucketVersioningStatusSuspended:
		return "Suspended", nil
	default:
		return "Disabled", nil
	}
}
func verifyRegion(ctx context.Context, c *s3.Client, bucket, expectedRegion string) error {
	out, err := c.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		return fmt.Errorf("HeadBucket: %w", err)
	}
	actual := aws.ToString(out.BucketRegion)
	if actual == "" {
		actual = "us-east-1"
	}
	if actual != expectedRegion {
		return fmt.Errorf("bucket %q is in %s, not %s", bucket, actual, expectedRegion)
	}
	log.Printf("Bucket region verified: %s", actual)
	return nil
}
func checkObjectLock(ctx context.Context, c *s3.Client, bucket string) error {
	out, err := c.GetObjectLockConfiguration(ctx, &s3.GetObjectLockConfigurationInput{Bucket: aws.String(bucket)})
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "ObjectLockConfigurationNotFoundError") ||
			strings.Contains(errMsg, "ObjectLockConfigurationNotFound") {
			log.Printf("Object Lock not enabled on %s", bucket)
			return nil
		}
		return fmt.Errorf("GetObjectLockConfiguration: %w", err)
	}
	if out.ObjectLockConfiguration != nil &&
		out.ObjectLockConfiguration.ObjectLockEnabled == st.ObjectLockEnabledEnabled {
		return fmt.Errorf("bucket %q has Object Lock enabled; review retention/legal-hold requirements before deletion", bucket)
	}
	return nil
}
func parsePrefixes(s string) []string {
	var r []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			r = append(r, p)
		}
	}
	return r
}
