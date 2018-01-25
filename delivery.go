package rmq

import (
	"fmt"
	"time"

	"gopkg.in/redis.v5"
)

type Delivery interface {
	Payload() string
	Ack() bool
	Reject() bool
	Delay(delayedAt time.Time, payload string) bool
	Push() bool
}

type wrapDelivery struct {
	payload     string
	unackedKey  string
	rejectedKey string
	pushKey     string
	delayedKey  string
	redisClient *redis.Client
}

func newDelivery(payload, unackedKey, rejectedKey, pushKey string, delayedKey string, redisClient *redis.Client) *wrapDelivery {
	return &wrapDelivery{
		payload:     payload,
		unackedKey:  unackedKey,
		rejectedKey: rejectedKey,
		pushKey:     pushKey,
		delayedKey:  delayedKey,
		redisClient: redisClient,
	}
}

func (delivery *wrapDelivery) String() string {
	return fmt.Sprintf("[%s %s]", delivery.payload, delivery.unackedKey)
}

func (delivery *wrapDelivery) Payload() string {
	return delivery.payload
}

func (delivery *wrapDelivery) Ack() bool {
	// debug(fmt.Sprintf("delivery ack %s", delivery)) // COMMENTOUT

	result := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	if redisErrIsNil(result) {
		return false
	}

	return result.Val() == 1
}

func (delivery *wrapDelivery) Reject() bool {
	return delivery.move(delivery.rejectedKey)
}

func (delivery *wrapDelivery) Push() bool {
	if delivery.pushKey != "" {
		return delivery.move(delivery.pushKey)
	} else {
		return delivery.move(delivery.rejectedKey)
	}
}

func (delivery *wrapDelivery) Delay(delayedAt time.Time, payload string) bool {
	z := redis.Z{
		Score: float64(delayedAt.Unix()),
		Member: payload,
	}
	if redisErrIsNil(delivery.redisClient.ZAdd(delivery.delayedKey, z)) {
		return false
	}

	if redisErrIsNil(delivery.redisClient.LRem(delivery.unackedKey, 1, payload)) {
		return false
	}

	// debug(fmt.Sprintf("delivery delayed %s", delivery)) // COMMENTOUT
	return true
}

func (delivery *wrapDelivery) move(key string) bool {
	if redisErrIsNil(delivery.redisClient.LPush(key, delivery.payload)) {
		return false
	}

	if redisErrIsNil(delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)) {
		return false
	}

	// debug(fmt.Sprintf("delivery rejected %s", delivery)) // COMMENTOUT
	return true
}
