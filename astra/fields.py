from astra import base_fields
from astra import validators


class CharField(validators.CharValidatorMixin, base_fields.BaseField):
    """
    setex : string
        用法： setex key seconds value
        作用： 设置key的值为value, 并且同时设置key的过期时间为 seconds 秒钟
        返回值： 设置成功返回 "ok"; 如果 key 已存在，则value 值覆盖原来的值
                 当 seconds 不合法时，会报错 (error) ERR value is not an integer or out of range

    setnx : string
        用法：setnx key value
        作用: setnx --> set if key not exist 如果 key存在，设置key 值为value; 如果 key 存在，什么也不做
        返回值：1 —— key存在且被设置了； 0 —— key不存在，没有被设置

    append:
        用法： append key value
        作用： 为指定的key追加值，如果key不存在相当于set key value, 如果key存在，是一个字符串，那么value追加在原来值的末尾
        返回值：int, 追加值后 key的长度

    bitcount:
        用法：bitcount key [start end]
        作用：统计字符串中值为 1 的bit数
        返回值：int, 0 —— key不存在返回

    getbit:
        用法：getbit key offset
        作用： 对于指定key存储的字符串，获取指定偏移量上的值;若offset超过字符串长度或者key不存在返回0
        返回值：int, 字符串指定偏移量上的bit(位)

    getrange：
        用法：getrange key start end
        作用：返回存储在key中的字符串的子串，有start 和 end决定偏移量
        返回值：string, 返回截取到的字符串，如果key不存在，则返回空字符串

    setbit:
        用法： setbit key offset value
        作用: 对key所存储的字符串，设置或者清除指定偏移量上的位
              根据value值为 1 或 0来判断是设置还是清除，如果key不存在，会创建一个新的字符串
              如果offset超过字符串的长度，字符串会增长，以确保offset上的存储值，其他用0填充
        返回值：int, 存储在offset上原来的值

    setrange:
        用法： setrange key offset value
        作用： 对于key存储的数值，从offset开始，使用value覆盖原来的存储值
              如果key不存在，按空字符串处理，自动填充的部分用零字节（‘\x00’）
        返回值：int, 修改后字符串的长度

    strlen:
        用法：strlen key
        作用：返回key存储的字符串的长度，如果key存储的不是字符串，会报错
        返回值：int, 0 —— 如果key不存在，返回 0

    expire:
        用法： expire key seconds
        作用： 设置key的过期时间，超过过期时间，key会被自动删除
        返回值： 1 —— 设置超时时间成功    0 —— key不存在

    ttl:
        用法： ttl key
        作用： 查看key的剩余过期时间
        返回值：int, -1 —— key不存在， -2 —— key没有关联超时时间时间
    """
    directly_redis_helpers = ('setex', 'setnx', 'append', 'bitcount',
                              'getbit', 'getrange', 'setbit', 'setrange',
                              'strlen', 'expire', 'ttl')


class BooleanField(validators.BooleanValidatorMixin, base_fields.BaseField):
    directly_redis_helpers = ('setex', 'setnx', 'expire', 'ttl',)


class IntegerField(validators.IntegerValidatorMixin, base_fields.BaseField):
    """
    incr:
        用法： incr key
        作用： 对key存储的值增一，如果key存储的值不能进行加法，会报错
               如果key不存在，会先初始化key的值为0
        返回值：int,执行incr后value的值

    incrby
        用法： incrby key increment
        作用：对key存储的值增指定值，如果key存储的值不能进行加法，会报错
               如果key不存在，会先初始化key的值为0
        返回值：int,执行incr后value的值

    getset:
        用法： getset key value
        作用： 设置key的值为value, 如果key不存在，返回 nil
              如果key存在，但存储的不是字符串会报错
        返回值： 返回设置前的值


    """
    directly_redis_helpers = ('setex', 'setnx', 'incr', 'incrby', 'decr',
                              'decrby', 'getset', 'expire', 'ttl',)


class ForeignField(validators.ForeignObjectValidatorMixin,
                   base_fields.BaseField):
    def assign(self, value):
        if value is None:  # Remove field when None was passed
            self.db.delete(self.get_key_name())
        else:
            super(ForeignField, self).assign(value)

    def obtain(self):
        """
        Convert saved pk to target object
        """
        if not self._to:
            raise RuntimeError('Relation model is not loaded')
        value = super(ForeignField, self).obtain()
        return self._to_wrapper(value)


class ForeignKey(ForeignField):  # legacy alias
    pass


class DateField(validators.DateValidatorMixin, base_fields.BaseField):
    directly_redis_helpers = ('setex', 'setnx', 'expire', 'ttl',)


class DateTimeField(validators.DateTimeValidatorMixin, base_fields.BaseField):
    directly_redis_helpers = ('setex', 'setnx', 'expire', 'ttl',)


class EnumField(validators.EnumValidatorMixin, base_fields.BaseField):
    pass


# Hashes
class CharHash(validators.CharValidatorMixin, base_fields.BaseHash):
    pass


class BooleanHash(validators.BooleanValidatorMixin, base_fields.BaseHash):
    pass


class IntegerHash(validators.IntegerValidatorMixin, base_fields.BaseHash):
    pass


class DateHash(validators.DateValidatorMixin, base_fields.BaseHash):
    pass


class DateTimeHash(validators.DateTimeValidatorMixin, base_fields.BaseHash):
    pass


class EnumHash(validators.EnumValidatorMixin, base_fields.BaseHash):
    pass


class ForeignHash(validators.ForeignObjectValidatorMixin,
                  base_fields.BaseHash):
    def assign(self, value):
        if value is None:  # Remove hash key when None was passed
            super(ForeignHash, self).remove()
        else:
            super(ForeignHash, self).assign(value)

    def obtain(self):
        """
        Convert saved pk to target object
        """
        if not self._to:
            raise RuntimeError('Relation model is not loaded')
        value = super(ForeignHash, self).obtain()
        return self._to_wrapper(value)


class List(base_fields.BaseCollection):
    """
    :
    """
    field_type_name = 'list'

    _allowed_redis_methods = ('lindex', 'linsert', 'llen', 'lpop', 'lpush',
                              'lpushx', 'lrange', 'lrem', 'lset', 'ltrim',
                              'rpop', 'rpoplpush', 'rpush', 'rpushx',)
    _single_object_answered_redis_methods = ('lindex', 'lpop', 'rpop',)
    _list_answered_redis_methods = ('lrange',)

    def __len__(self):
        return self.llen()

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.lrange(item.start, item.stop)
        else:
            ret = self.lrange(item, item)
            return ret[0] if len(ret) == 1 else None


class Set(base_fields.BaseCollection):
    field_type_name = 'set'
    _allowed_redis_methods = ('sadd', 'scard', 'sdiff', 'sdiffstore', 'sinter',
                              'sinterstore', 'sismember', 'smembers', 'smove',
                              'spop', 'srandmember', 'srem', 'sscan', 'sunion',
                              'sunionstore')
    _single_object_answered_redis_methods = ('spop',)
    _list_answered_redis_methods = ('sdiff', 'sinter', 'smembers',
                                    'srandmember', 'sscan', 'sunion',)

    def __len__(self):
        return self.scard()


class SortedSet(base_fields.BaseCollection):
    field_type_name = 'zset'
    _allowed_redis_methods = ('bzpopmax', 'bzpopmin', 'zadd', 'zcard',
                              'zcount', 'zincrby', 'zinterstore', 'zlexcount',
                              'zrange', 'zpopmax', 'zpopmin', 'zrangebylex',
                              'zrangebyscore', 'zrank', 'zrem',
                              'zremrangebylex', 'zremrangebyrank',
                              'zremrangebyscore', 'zrevrange',
                              'zrevrangebylex', 'zrevrangebyscore', 'zrevrank',
                              'zscan', 'zscore', 'zunionstore')
    _single_object_answered_redis_methods = ()
    _list_answered_redis_methods = ('zpopmax', 'zpopmin', 'zrange',
                                    'zrangebylex', 'zrangebyscore',
                                    'zrevrange', 'zrevrangebylex',
                                    'zrevrangebyscore', 'zscan',)

    def __len__(self):
        return self.zcard()

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.zrangebyscore(item.start or '-inf',
                                      item.stop or '+inf')
        else:
            ret = self.zrangebyscore(item, item)
            return ret[0] if len(ret) == 1 else None
