package datastore

import (
	"github.com/AlphaMinZ/myredis_go/database"
	"github.com/AlphaMinZ/myredis_go/handler"
)

func (k *KVStore) getAsSet(key string) (Set, error) {
	v, ok := k.data[key]
	if !ok {
		return nil, nil
	}

	set, ok := v.(Set)
	if !ok {
		return nil, handler.NewWrongTypeErrReply()
	}

	return set, nil
}

func (k *KVStore) putAsSet(key string, set Set) {
	k.data[key] = set
}

type Set interface {
	Add(value string) int64
	Exist(value string) int64
	Rem(value string) int64
	database.CmdAdapter
}

type setEntity struct {
	key       string
	container map[string]struct{}
}

func newSetEntity(key string) Set {
	return &setEntity{
		key:       key,
		container: make(map[string]struct{}),
	}
}

func (s *setEntity) Add(value string) int64 {
	if _, ok := s.container[value]; ok {
		return 0
	}
	s.container[value] = struct{}{}
	return 1
}

func (s *setEntity) Exist(value string) int64 {
	if _, ok := s.container[value]; ok {
		return 1
	}
	return 0
}

func (s *setEntity) Rem(value string) int64 {
	if _, ok := s.container[value]; ok {
		delete(s.container, value)
		return 1
	}
	return 0
}

func (s *setEntity) ToCmd() [][]byte {
	args := make([][]byte, 0, 2+len(s.container))
	args = append(args, []byte(database.CmdTypeSAdd), []byte(s.key))
	for k := range s.container {
		args = append(args, []byte(k))
	}

	return args
}
