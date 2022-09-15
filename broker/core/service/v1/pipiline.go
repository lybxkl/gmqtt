package service

import (
	atomic2 "sync/atomic"
	"time"

	"github.com/bsm/ratelimit"
	"go.uber.org/atomic"
)

var (
	serviceBusy                        *atomic.Bool = atomic.NewBool(false) // 服务端正忙 CONNACK, DISCONNECT
	serverBeingShutDown                *atomic.Bool = atomic.NewBool(false) // 服务端关闭中 DISCONNECT
	exceededConnectionRateLimit        *atomic.Bool = atomic.NewBool(false) // 超出连接速率限制 CONNACK, DISCONNECT
	unsupportedSubscriptionIdentifiers *atomic.Bool = atomic.NewBool(false) // 不支持订阅标识符 SUBACK, DISCONNECT
	unsupportedWildcardSubscriptions   *atomic.Bool = atomic.NewBool(false) // 不支持通配符订阅 SUBACK, DISCONNECT
	unsupportedSharedSubscriptions     *atomic.Bool = atomic.NewBool(false) // 不支持共享订阅 SUBACK, DISCONNECT
	unsupportedRetention               *atomic.Bool = atomic.NewBool(false) // 不支持保留 CONNACK, DISCONNECT
)

// ServiceBusy 服务端正忙 CONNACK, DISCONNECT
func ServiceBusy() bool {
	return serviceBusy.Load()
}

func SetServiceBusy(sb bool) {
	serviceBusy.Store(sb)
}

// ServerBeingShutDown 服务端关闭中 DISCONNECT
func ServerBeingShutDown() bool {
	return serverBeingShutDown.Load()
}

func SetServerBeingShutDown(sbsd bool) {
	serverBeingShutDown.Store(sbsd)
}

// ExceededConnectionRateLimit 超出连接速率限制 CONNACK, DISCONNECT
func ExceededConnectionRateLimit() bool {
	return exceededConnectionRateLimit.Load()
}

func SetExceededConnectionRateLimit(ecrl bool) {
	exceededConnectionRateLimit.Store(ecrl)
}

// UnsupportedSubscriptionIdentifiers 不支持订阅标识符 SUBACK, DISCONNECT
func UnsupportedSubscriptionIdentifiers() bool {
	return unsupportedSubscriptionIdentifiers.Load()
}

func SetUnsupportedSubscriptionIdentifiers(sui bool) {
	unsupportedSubscriptionIdentifiers.Store(sui)
}

// UnsupportedWildcardSubscriptions 不支持通配符订阅 SUBACK, DISCONNECT
func UnsupportedWildcardSubscriptions() bool {
	return unsupportedWildcardSubscriptions.Load()
}

func SetUnsupportedWildcardSubscriptions(uws bool) {
	unsupportedWildcardSubscriptions.Store(uws)
}

// UnsupportedSharedSubscriptions 不支持共享订阅 SUBACK, DISCONNECT
func UnsupportedSharedSubscriptions() bool {
	return unsupportedSharedSubscriptions.Load()
}

func SetUnsupportedSharedSubscriptions(uss bool) {
	unsupportedSharedSubscriptions.Store(uss)
}

// UnsupportedRetention 不支持保留 CONNACK, DISCONNECT
func UnsupportedRetention() bool {
	return unsupportedRetention.Load()
}

func SetUnsupportedRetention(ur bool) {
	unsupportedRetention.Store(ur)
}

// Sign 用于三个处理协程的通讯
type Sign struct {
	beyondQuota     bool // 超过配额信号 CONNACK, PUBACK, PUBREC, SUBACK, DISCONNECT
	tooManyMessages bool // 消息太过频繁 DISCONNECT
	rateLimit       *ratelimit.RateLimiter
	quota           int64 // 初始配额
	currQuota       int64 // 当前可使用配额
	limit           int
}

func NewSign(quota int64, limit int) *Sign {
	if quota == 0 && limit == 0 {
		return &Sign{
			rateLimit: ratelimit.New(100000, time.Second),
			quota:     100000,
			limit:     100000,
			currQuota: 100000,
		}
	}
	rl := ratelimit.New(limit, time.Second)
	return &Sign{
		rateLimit: rl,
		quota:     quota,
		limit:     limit,
		currQuota: quota,
	}
}

func (s *Sign) UpLimit(limit int) {
	s.limit = limit
	s.rateLimit.UpdateRate(limit)
}
func (s *Sign) UpQuota(quota int64, reset bool) {
	atomic2.SwapInt64(&s.quota, quota)
	if reset {
		s.setBeyondQuota(false)
		atomic2.SwapInt64(&s.currQuota, quota)
	} else if s.quota < s.currQuota {
		s.setBeyondQuota(true)
	}
}

// Limit 限流了
func (s *Sign) Limit() bool {
	if s.rateLimit.Limit() {
		s.setTooManyMessages(true)
		return true
	}
	return false
}

// BeyondQuota 超过配额信号 CONNACK, PUBACK, PUBREC, SUBACK, DISCONNECT
func (s *Sign) BeyondQuota() bool {
	return s.beyondQuota
}

// ReqQuota 请求一个配额,发送端请求，因为是发送配额
// 只会在qos 1, 2处理
func (s *Sign) ReqQuota() bool {
	//println("请求一次")
	if s.beyondQuota {
		return false
	}
	cur := atomic2.AddInt64(&s.currQuota, -1)
	if cur < 0 {
		s.setBeyondQuota(true)
		atomic2.SwapInt64(&s.currQuota, 0)
		return false
	}
	return true
}

// AddQuota 新增一个配额
// 1. 每当收到一个PUBACK报文或PUBCOMP报文，不管PUBACK或PUBCOMP报文是否包含错误码。
// 2. 每次收到一个包含返回码大于等于0x80的PUBREC报文。
// 在初始发送配额之上尝试增加配额可能是由建立新的网络连接后重新发送PUBREL数据包引起的。
func (s *Sign) AddQuota() {
	//println("释放一次")
	cur := atomic2.AddInt64(&s.currQuota, 1)
	if cur > 0 && s.beyondQuota {
		s.setBeyondQuota(false)
	}
}
func (s *Sign) setBeyondQuota(beyondQuota bool) {
	s.beyondQuota = beyondQuota
}

// TooManyMessages 消息太过频繁 DISCONNECT
func (s *Sign) TooManyMessages() bool {
	return s.tooManyMessages
}

func (s *Sign) setTooManyMessages(tooManyMessages bool) {
	s.tooManyMessages = tooManyMessages
}
