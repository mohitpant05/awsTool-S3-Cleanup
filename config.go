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
