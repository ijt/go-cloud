// Copyright 2018 The Go Cloud Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3blob

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/blob/drivertest"
	"gocloud.dev/internal/testing/setup"
)

// These constants record the region & bucket used for the last --record.
// If you want to use --record mode,
// 1. Create a bucket in your AWS project from the S3 management console.
//    https://s3.console.aws.amazon.com/s3/home.
// 2. Update this constant to your bucket name.
// TODO(issue #300): Use Terraform to provision a bucket, and get the bucket
//    name from the Terraform output instead (saving a copy of it for replay).
const (
	bucketName = "go-cloud-bucket"
	region     = "us-east-2"
)

type harness struct {
	session *session.Session
	rt      http.RoundTripper
	closer  func()
}

func newHarness(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
	sess, rt, done := setup.NewAWSSession(t, region)
	return &harness{session: sess, rt: rt, closer: done}, nil
}

func (h *harness) HTTPClient() *http.Client {
	return &http.Client{Transport: h.rt}
}

func (h *harness) MakeDriver(ctx context.Context) (driver.Bucket, error) {
	return openBucket(ctx, h.session, bucketName, nil)
}

func (h *harness) Close() {
	h.closer()
}

func TestConformance(t *testing.T) {
	drivertest.RunConformanceTests(t, newHarness, []drivertest.AsTest{verifyContentLanguage{}})
}

const language = "nl"

// verifyContentLanguage uses As to access the underlying GCS types and
// read/write the ContentLanguage field.
type verifyContentLanguage struct{}

func (verifyContentLanguage) Name() string {
	return "verify ContentLanguage can be written and read through As"
}

func (verifyContentLanguage) BucketCheck(b *blob.Bucket) error {
	var client *s3.S3
	if !b.As(&client) {
		return errors.New("Bucket.As failed")
	}
	return nil
}

func (verifyContentLanguage) ErrorCheck(err error) error {
	var e awserr.Error
	if !blob.ErrorAs(err, &e) {
		return errors.New("blob.ErrorAs failed")
	}
	return nil
}

func (verifyContentLanguage) BeforeWrite(as func(interface{}) bool) error {
	var req *s3manager.UploadInput
	if !as(&req) {
		return errors.New("Writer.As failed")
	}
	req.ContentLanguage = aws.String(language)
	return nil
}

func (verifyContentLanguage) BeforeList(as func(interface{}) bool) error {
	var req *s3.ListObjectsV2Input
	if !as(&req) {
		return errors.New("List.As failed")
	}
	// Nothing to do.
	return nil
}

func (verifyContentLanguage) AttributesCheck(attrs *blob.Attributes) error {
	var hoo s3.HeadObjectOutput
	if !attrs.As(&hoo) {
		return errors.New("Attributes.As returned false")
	}
	if got := *hoo.ContentLanguage; got != language {
		return fmt.Errorf("got %q want %q", got, language)
	}
	return nil
}

func (verifyContentLanguage) ReaderCheck(r *blob.Reader) error {
	var goo s3.GetObjectOutput
	if !r.As(&goo) {
		return errors.New("Reader.As returned false")
	}
	if got := *goo.ContentLanguage; got != language {
		return fmt.Errorf("got %q want %q", got, language)
	}
	return nil
}

func (verifyContentLanguage) ListObjectCheck(o *blob.ListObject) error {
	var obj s3.Object
	if !o.As(&obj) {
		return errors.New("ListObject.As returned false")
	}
	// Nothing to check.
	return nil
}

func TestOpenBucket(t *testing.T) {
	tests := []struct {
		description string
		bucketName  string
		nilSession  bool
		want        string
		wantErr     bool
	}{
		{
			description: "empty bucket name results in error",
			wantErr:     true,
		},
		{
			description: "nil sess results in error",
			bucketName:  "foo",
			nilSession:  true,
			wantErr:     true,
		},
		{
			description: "success",
			bucketName:  "foo",
			want:        "foo",
		},
	}

	ctx := context.Background()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			var sess client.ConfigProvider
			if !test.nilSession {
				var done func()
				sess, _, done = setup.NewAWSSession(t, region)
				defer done()
			}

			// Create driver impl.
			drv, err := openBucket(ctx, sess, test.bucketName, nil)
			if (err != nil) != test.wantErr {
				t.Errorf("got err %v want error %v", err, test.wantErr)
			}
			if drv != nil {
				if drv.name != test.want {
					t.Errorf("got %q want %q", drv.name, test.want)
				}
			}

			// Create concrete type.
			_, err = OpenBucket(ctx, sess, test.bucketName, nil)
			if (err != nil) != test.wantErr {
				t.Errorf("got err %v want error %v", err, test.wantErr)
			}
		})
	}
}

func TestOpenURL(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		url      string
		wantName string
		wantErr  bool
	}{
		{
			url:      "s3://mybucket?region=foo",
			wantName: "mybucket",
		},
		{
			url:      "s3://mybucket2?region=bar",
			wantName: "mybucket2",
		},
	}

	for _, test := range tests {
		t.Run(test.url, func(t *testing.T) {
			u, err := url.Parse(test.url)
			if err != nil {
				t.Fatal(err)
			}
			got, err := openURL(ctx, u)
			if (err != nil) != test.wantErr {
				t.Errorf("got err %v want error %v", err, test.wantErr)
			}
			if err != nil {
				return
			}
			gotB, ok := got.(*bucket)
			if !ok {
				t.Fatalf("got type %T want *bucket", got)
			}
			if gotB.name != test.wantName {
				t.Errorf("got bucket name %q want %q", gotB.name, test.wantName)
			}
		})
	}
}
