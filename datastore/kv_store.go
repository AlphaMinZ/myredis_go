package datastore

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AlphaMinZ/myredis_go/database"
	"github.com/AlphaMinZ/myredis_go/handler"
	"github.com/AlphaMinZ/myredis_go/lib"
)

type KVStore struct {
	mu        sync.Mutex
	data      map[string]interface{}
	expiredAt map[string]time.Time
}

func NewKVStore() database.DataStore {
	return &KVStore{
		data:      make(map[string]interface{}),
		expiredAt: make(map[string]time.Time),
	}
}

func (k *KVStore) Expire(args [][]byte) handler.Reply {
	key := string(args[0])
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return handler.NewSyntaxErrReply()
	}
	if ttl <= 0 {
		return handler.NewErrReply("ERR invalid expire time")
	}
	k.expire(key, lib.TimeNow().Add(time.Duration(ttl)*time.Second))
	return handler.NewOKReply()
}

func (k *KVStore) Get(args [][]byte) handler.Reply {
	key := string(args[0])
	v, err := k.getAsString(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}
	return handler.NewBulkReply(v.Bytes())
}

func (k *KVStore) MGet(args [][]byte) handler.Reply {
	res := make([][]byte, len(args))
	for _, arg := range args {
		v, err := k.getAsString(string(arg))
		if err != nil {
			return handler.NewErrReply(err.Error())
		}
		res = append(res, v.Bytes())
	}

	return handler.NewMultiBulkReply(res)
}

func (k *KVStore) Set(args [][]byte) handler.Reply {
	key := string(args[0])
	value := string(args[1])

	var (
		insertStrategy bool
		ttlStrategy    bool
		ttlSeconds     int64
	)

	for i := 2; i < len(args); i++ {
		flag := strings.ToLower(string(args[i]))
		switch flag {
		case "nx":
			insertStrategy = true
		case "ex":
			if i == len(args)-1 {
				return handler.NewSyntaxErrReply()
			}
			ttl, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				return handler.NewSyntaxErrReply()
			}
			if ttl <= 0 {
				return handler.NewErrReply("ERR invalid expire time")
			}

			ttlStrategy = true
			ttlSeconds = ttl
			i++
		default:
			return handler.NewSyntaxErrReply()
		}
	}

	affected := k.put(key, value, insertStrategy)
	if affected > 0 && ttlStrategy {
		k.expire(key, lib.TimeNow().Add(time.Duration(ttlSeconds)*time.Second))
	}

	if affected > 0 {
		return handler.NewIntReply(affected)
	}

	return handler.NewNillReply()
}

func (k *KVStore) MSet(args [][]byte) handler.Reply {
	if len(args)&1 == 1 {
		return handler.NewSyntaxErrReply()
	}

	for i := 0; i < len(args); i += 2 {
		_ = k.put(string(args[i]), string(args[i+1]), false)
	}

	return handler.NewIntReply(int64(len(args)) >> 1)
}

func (k *KVStore) LPush(args [][]byte) handler.Reply {
	key := string(args[0])
	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		list = newListEntity()
		k.putAsList(key, list)
	}

	for i := 1; i < len(args); i++ {
		list.LPush(args[i])
	}

	return handler.NewIntReply(list.Len())
}

func (k *KVStore) LPop(args [][]byte) handler.Reply {
	key := string(args[0])
	var cnt int64
	if len(args) > 1 {
		rawCnt, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return handler.NewSyntaxErrReply()
		}
		if rawCnt < 1 {
			return handler.NewSyntaxErrReply()
		}
		cnt = rawCnt
	}

	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		return handler.NewNillReply()
	}

	if cnt == 0 {
		cnt = 1
	}

	poped := list.LPop(cnt)
	if poped == nil {
		return handler.NewNillReply()
	}

	if len(poped) == 1 {
		return handler.NewBulkReply(poped[0])
	}

	return handler.NewMultiBulkReply(poped)
}

func (k *KVStore) RPush(args [][]byte) handler.Reply {
	key := string(args[0])
	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		list = newListEntity(args[1:]...)
		k.putAsList(key, list)
		return handler.NewIntReply(list.Len())
	}

	for i := 1; i < len(args); i++ {
		list.RPush(args[i])
	}

	return handler.NewIntReply(list.Len())
}

func (k *KVStore) RPop(args [][]byte) handler.Reply {
	key := string(args[0])
	var cnt int64
	if len(args) > 1 {
		rawCnt, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return handler.NewSyntaxErrReply()
		}
		if rawCnt < 1 {
			return handler.NewSyntaxErrReply()
		}
		cnt = rawCnt
	}

	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		return handler.NewNillReply()
	}

	if cnt == 0 {
		cnt = 1
	}

	poped := list.RPop(cnt)
	if poped == nil {
		return handler.NewNillReply()
	}

	if len(poped) == 1 {
		return handler.NewBulkReply(poped[0])
	}

	return handler.NewMultiBulkReply(poped)
}

func (k *KVStore) LRange(args [][]byte) handler.Reply {
	if len(args) != 3 {
		return handler.NewSyntaxErrReply()
	}

	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return handler.NewSyntaxErrReply()
	}

	stop, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return handler.NewSyntaxErrReply()
	}

	list, err := k.getAsList(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if list == nil {
		return handler.NewNillReply()
	}

	if got := list.Range(start, stop); got != nil {
		return handler.NewMultiBulkReply(got)
	}

	return handler.NewNillReply()
}

// set
func (k *KVStore) SAdd(args [][]byte) handler.Reply {
	key := string(args[0])
	set, err := k.getAsSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}
	if set == nil {
		set = newSetEntity()
		k.putAsSet(key, set)
	}

	var added int64
	for _, arg := range args[1:] {
		added += set.Add(string(arg))
	}

	return handler.NewIntReply(added)
}

func (k *KVStore) SIsMember(args [][]byte) handler.Reply {
	key := string(args[0])
	set, err := k.getAsSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if set == nil {
		return handler.NewIntReply(0)
	}

	return handler.NewIntReply(set.Exist(string(args[1])))
}

func (k *KVStore) SRem(args [][]byte) handler.Reply {
	key := string(args[0])
	set, err := k.getAsSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if set == nil {
		return handler.NewIntReply(0)
	}

	var remed int64
	for _, arg := range args[1:] {
		remed += set.Rem(string(arg))
	}

	return handler.NewIntReply(remed)
}

// hash
func (k *KVStore) HSet(args [][]byte) handler.Reply {
	if len(args)&1 != 1 {
		return handler.NewSyntaxErrReply()
	}

	key := string(args[0])
	hmap, err := k.getAsHashMap(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if hmap == nil {
		hmap = newHashMapEntity()
		k.putAsHashMap(key, hmap)
	}

	for i := 0; i < len(args)-1; i += 2 {
		hkey := string(args[i+1])
		hvalue := args[i+2]
		hmap.Put(hkey, hvalue)
	}

	return handler.NewIntReply(int64((len(args) - 1) >> 1))
}

func (k *KVStore) HGet(args [][]byte) handler.Reply {
	key := string(args[0])
	hmap, err := k.getAsHashMap(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if hmap == nil {
		return handler.NewNillReply()
	}

	if v := hmap.Get(string(args[1])); v != nil {
		return handler.NewBulkReply(v)
	}

	return handler.NewNillReply()
}

func (k *KVStore) HDel(args [][]byte) handler.Reply {
	key := string(args[0])
	hmap, err := k.getAsHashMap(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if hmap == nil {
		return handler.NewIntReply(0)
	}

	var remed int64
	for _, arg := range args[1:] {
		remed += hmap.Del(string(arg))
	}
	return handler.NewIntReply(remed)
}

// sorted set
func (k *KVStore) ZAdd(args [][]byte) handler.Reply {
	if len(args)&1 != 1 {
		return handler.NewSyntaxErrReply()
	}

	key := string(args[0])
	var (
		scores  = make([]int64, 0, (len(args)-1)>>1)
		members = make([]string, 0, (len(args)-1)>>1)
	)

	for i := 0; i < len(args)-1; i += 2 {
		score, err := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err != nil {
			return handler.NewSyntaxErrReply()
		}

		scores = append(scores, score)
		members = append(members, string(args[i+2]))
	}

	zset, err := k.getAsSortedSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if zset == nil {
		zset = newSkiplist()
		k.putAsSortedSet(key, zset)
	}

	for i := 0; i < len(scores); i++ {
		zset.Add(scores[i], members[i])
	}

	return handler.NewIntReply(int64(len(scores)))
}

func (k *KVStore) ZRangeByScore(args [][]byte) handler.Reply {
	if len(args) < 3 {
		return handler.NewSyntaxErrReply()
	}

	key := string(args[0])
	score1, err := strconv.ParseInt(string(args[1]), 10, 65)
	if err != nil {
		return handler.NewSyntaxErrReply()
	}
	score2, err := strconv.ParseInt(string(args[2]), 10, 65)
	if err != nil {
		return handler.NewSyntaxErrReply()
	}

	zset, err := k.getAsSortedSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if zset == nil {
		return handler.NewNillReply()
	}

	rawRes := zset.Range(score1, score2)
	if len(rawRes) == 0 {
		return handler.NewNillReply()
	}

	res := make([][]byte, 0, len(rawRes))
	for _, item := range rawRes {
		res = append(res, []byte(item))
	}

	return handler.NewMultiBulkReply(res)
}

func (k *KVStore) ZRem(args [][]byte) handler.Reply {
	key := string(args[0])
	zset, err := k.getAsSortedSet(key)
	if err != nil {
		return handler.NewErrReply(err.Error())
	}

	if zset == nil {
		return handler.NewIntReply(0)
	}

	var remed int64
	for _, arg := range args {
		remed += zset.Rem(string(arg))
	}

	return handler.NewIntReply(remed)
}
