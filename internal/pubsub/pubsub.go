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
package pubsub

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/go-cloud/internal/pubsub/driver"
	"github.com/google/go-cloud/internal/retry"
	gax "github.com/googleapis/gax-go"
	"google.golang.org/api/support/bundler"
)

// Message contains data to be published.
type Message struct {
	// Body contains the content of the message.
	Body []byte

	// Metadata has key/value metadata for the message.
	Metadata map[string]string

	// ack is a closure that queues this message for acknowledgement.
	ack func()
}

// Ack acknowledges the message, telling the server that it does not need to be
// sent again to the associated Subscription. It returns immediately, but the
// actual ack is sent in the background, and is not guaranteed to succeed.
func (m *Message) Ack() {
	go m.ack()
}

// Topic publishes messages to all its subscribers.
type Topic struct {
	driver  driver.Topic
	batcher driver.Batcher
	mu      sync.Mutex
	err     error
}

type msgErrChan struct {
	msg     *Message
	errChan chan error
}

// Send publishes a message. It only returns after the message has been
// sent, or failed to be sent. Send can be called from multiple goroutines
// at once.
func (t *Topic) Send(ctx context.Context, m *Message) error {
	// Check for doneness before we do any work.
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	err := t.err
	t.mu.Unlock()
	if err != nil {
		return err
	}
	mec := msgErrChan{
		msg:     m,
		errChan: make(chan error),
	}
	if err := t.batcher.Add(ctx, mec); err != nil {
		return err
	}
	return <-mec.errChan
}

// Close flushes pending message sends and disconnects the Topic.
// It only returns after all pending messages have been sent.
func (t *Topic) Close() error {
	t.mu.Lock()
	t.err = errors.New("pubsub: Topic closed")
	t.mu.Unlock()
	t.batcher.Shutdown()
	return nil
}

// NewTopicWithBatcher makes a pubsub.Topic from a driver.Topic. This
// constructor creates a default batcher for sending messages.
// It is for use by provider implementations.
func NewTopic(d driver.Topic) *Topic {
	return NewTopicWithBatcher(d, NewSendBatcher(d))
}

// NewTopicWithBatcher makes a pubsub.Topic from a driver.Topic and a
// driver.Batcher.
// It is for use by provider implementations.
func NewTopicWithBatcher(d driver.Topic, b driver.Batcher) *Topic {
	return &Topic{
		driver:  d,
		batcher: b,
	}
}

type sendBatcher struct {
	b    *bundler.Bundler
	done bool
}

func (sb *sendBatcher) Add(ctx context.Context, item interface{}) error {
	if sb.done {
		return errors.New("tried to add an item to a send batcher that has been shut down")
	}
	mec := item.(msgErrChan)
	m := mec.msg
	size := len(m.Body)
	for k, v := range m.Metadata {
		size += len(k)
		size += len(v)
	}
	return sb.b.AddWait(ctx, item, size)
}

func (sb *sendBatcher) Shutdown() {
	sb.b.Flush()
	sb.done = true
}

// NewSendBatcher creates a Bundler for message sends.
// It is for use by provider implementations.
func NewSendBatcher(d driver.Topic) *sendBatcher {
	handler := func(item interface{}) {
		mecs, ok := item.([]msgErrChan)
		if !ok {
			panic("failed conversion to []msgErrChan in bundler handler")
		}
		var dms []*driver.Message
		for _, mec := range mecs {
			m := mec.msg
			dm := &driver.Message{
				Body:     m.Body,
				Metadata: m.Metadata,
			}
			dms = append(dms, dm)
		}

		callCtx := context.TODO()
		err := retry.Call(callCtx, gax.Backoff{}, d.IsRetryable, func() error {
			return d.SendBatch(callCtx, dms)
		})
		for _, mec := range mecs {
			mec.errChan <- err
		}
	}
	b := bundler.NewBundler(msgErrChan{}, handler)
	b.DelayThreshold = time.Millisecond
	return &sendBatcher{b: b}
}

// Subscription receives published messages.
type Subscription struct {
	driver driver.Subscription

	// ackBatcher makes batches of acks and sends them to the server.
	ackBatcher driver.Batcher

	mu sync.Mutex

	// q is the local queue of messages downloaded from the server.
	q   []*Message
	err error
}

// Receive receives and returns the next message from the Subscription's queue,
// blocking and polling if none are available. This method can be called
// concurrently from multiple goroutines. The Ack() method of the returned
// Message has to be called once the message has been processed, to prevent it
// from being received again.
func (s *Subscription) Receive(ctx context.Context) (*Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	if len(s.q) == 0 {
		if err := s.getNextBatch(ctx); err != nil {
			return nil, err
		}
	}
	m := s.q[0]
	s.q = s.q[1:]
	return m, nil
}

// getNextBatch gets the next batch of messages from the server and saves it in
// s.q.
func (s *Subscription) getNextBatch(ctx context.Context) error {
	var msgs []*driver.Message
	err := retry.Call(ctx, gax.Backoff{}, s.driver.IsRetryable, func() error {
		var err error
		// TODO(#691): dynamically adjust maxMessages
		const maxMessages = 10
		msgs, err = s.driver.ReceiveBatch(ctx, maxMessages)
		return err
	})
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return errors.New("subscription driver bug: received empty batch")
	}
	s.q = nil
	for _, m := range msgs {
		id := m.AckID
		s.q = append(s.q, &Message{
			Body:     m.Body,
			Metadata: m.Metadata,
			ack: func() {
				s.ackBatcher.Add(ctx, ackIDBox{id})
			},
		})
	}
	return nil
}

// Close flushes pending ack sends and disconnects the Subscription.
func (s *Subscription) Close() error {
	s.mu.Lock()
	s.err = errors.New("pubsub: Subscription closed")
	s.mu.Unlock()
	s.ackBatcher.Shutdown()
	return nil
}

// ackIDBox makes it possible to use a driver.AckID with bundler.
type ackIDBox struct {
	ackID driver.AckID
}

// NewSubscription creates a Subscription from a driver.Subscription. This
// constructor creates a default batcher to send acks back to the pubsub
// provider.
// It is for use by provider implementations.
func NewSubscription(d driver.Subscription) *Subscription {
	return NewSubscriptionWithBatcher(d, NewAckBatcher(d))
}

// NewSubscriptionWithBatcher creates a Subscription from a driver.Subscription
// and a driver.Batcher for acks.
// It is for use by provider implementations.
func NewSubscriptionWithBatcher(d driver.Subscription, ackBatcher driver.Batcher) *Subscription {
	return &Subscription{
		driver:     d,
		ackBatcher: ackBatcher,
	}
}

type ackBatcher struct {
	b    *bundler.Bundler
	done bool
}

func (ab *ackBatcher) Add(ctx context.Context, item interface{}) error {
	if ab.done {
		return errors.New("tried to add an item to an ack batcher that has been shut down")
	}
	// size is an estimate of the size of a single AckID in bytes.
	const size = 8
	return ab.b.Add(item, size)
}

func (ab *ackBatcher) Shutdown() {
	ab.b.Flush()
	ab.done = true
}

// NewAckBatcher creates a batcher for message acks.
// It is for use by provider implementations.
func NewAckBatcher(d driver.Subscription) *ackBatcher {
	handler := func(item interface{}) {
		boxes := item.([]ackIDBox)
		var ids []driver.AckID
		for _, box := range boxes {
			id := box.ackID
			ids = append(ids, id)
		}
		// TODO: Consider providing a way to stop this call. See #766.
		callCtx := context.Background()
		err := retry.Call(callCtx, gax.Backoff{}, d.IsRetryable, func() error {
			return d.SendAcks(callCtx, ids)
		})
		// TODO(#695): Do something sensible if SendAcks returns an error.
		_ = err
	}
	b := bundler.NewBundler(ackIDBox{}, handler)
	b.DelayThreshold = time.Millisecond
	return &ackBatcher{b: b}
}
