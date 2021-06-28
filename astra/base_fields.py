import datetime as dt
from astra.validators import ForeignObjectValidatorMixin


class ModelField(object):
    directly_redis_helpers = ()  # Direct method helpers
    field_type_name = '--'

    def __init__(self, **kwargs):
        """
        传入**kwargs 的demo
        eg： {
            "instance":True,
            "name":"username",
            "model":<Model UserObject(pk=1)>,
            "db":Redis<ConnectionPool<host=127.0.0.1, port=6379,db=0>>>,
            **target_field.options
        }
        """
        if 'instance' in kwargs:
            self.name = kwargs['name']
            self.model = kwargs['model']
            self.db = kwargs['db']
        self.options = kwargs

    def get_key_name(self, is_hash=False):
        """
        Create redis key. Schema:
        prefix::object_name::field_type::id::field_name, e.g.
            prefix::user::fld::12::login
            prefix::user::list::12::sites
            prefix::user::zset::12::winners
            prefix::user::hash::54
        """

        """
        传入的model的demo
        这里调用了self.model.get_key_prefix(); self.model.pk
        
        self.field_type_name = '--'  默认是两个横线
        子类都重写了这个参数
        """
        items = [self.model.get_key_prefix(),
                 self.field_type_name, str(self.model.pk)]
        if not is_hash:
            items.append(self.name)
        return '::'.join(items)

    def assign(self, value):
        """
        子类必须实现这个方法
        """
        raise NotImplementedError('Subclasses must implement assign')

    def obtain(self):
        """
        子类必须实现这个方法
        """
        raise NotImplementedError('Subclasses must implement obtain')

    def get_helper_func(self, method_name):
        """
        传入的method_name demo

         需要断点调试，重点查看command 最后拼成的结果
        """
        if method_name not in self.directly_redis_helpers:
            raise AttributeError('Invalid attribute with name "%s"'
                                 % (method_name,))
        original_command = getattr(self.db, method_name)
        current_key = self.get_key_name()

        def _method_wrapper(*args, **kwargs):
            new_args = [current_key]
            for v in args:
                new_args.append(v)
            return original_command(*new_args, **kwargs)

        return _method_wrapper

    def remove(self):
        """
        删除某个值

        Not found func Redis delete
        """
        self.db.delete(self.get_key_name())


# Fields:
class BaseField(ModelField):
    """
    继承了类  ModelField

    重新给 field_type_name 赋值

    """
    field_type_name = 'fld'

    def assign(self, value):
        """
        重写了父类的 assign 方法

        调用了 Redis.set(key, value)   -----  It's a func for string
        """
        saved_value = self._convert_set(value)
        self.db.set(self.get_key_name(), saved_value)

    def obtain(self):
        """
        重写了父类的 obtain 方法

        调用了 Redis.get(key)  It's a func for String
        """
        value = self.db.get(self.get_key_name())
        return self._convert_get(value)

    def _convert_set(self, value):
        """
        需要子类必须实现这个方法

        检查存入值在将数据发送给服务器之前
        """
        """ Check saved value before send to server """
        raise NotImplementedError('Subclasses must implement _convert_set')

    def _convert_get(self, value):
        """
        子类必须实现这个方法

        将 服务器端获取的数据 转化成 用户的查看的类型
        """
        """ Convert server answer to user type """
        raise NotImplementedError('Subclasses must implement _convert_get')


# Hashes
class BaseHash(ModelField):
    """
    继承了类  ModelField

    重新给 field_type_name 赋值
    """
    field_type_name = 'hash'

    def assign(self, value):
        """
        重写了父类的  assign 方法

        调用了 Redis.hset() 方法

        需要查看 self.model._astra_hash_loaded
        self.model._astra_hash
        self.model._astra_hash_exist
        """
        saved_value = self._convert_set(value)
        self.db.hset(self.get_key_name(True), self.name, saved_value)
        if self.model._astra_hash_loaded:
            self.model._astra_hash[self.name] = saved_value
        self.model._astra_hash_exist = True

    def obtain(self):
        """
        需要查看 self.model._astra_hash.get()
        """
        self._load_hash()
        return self._convert_get(self.model._astra_hash.get(self.name, None))

    def _load_hash(self):
        """
        需要查看
            self.model._astra_hash_loaded : Boolean
            self.model._astra_hash : [] 或者 {}
            self.model._astra_hash_exist : Boolean or None

        调用了 Redis.hgetall()  获取在哈希表中指定 key 的所有字段和值; 若key 不存在，返回空列表
        """
        if self.model._astra_hash_loaded:
            return
        self.model._astra_hash_loaded = True
        self.model._astra_hash = \
            self.db.hgetall(
                self.get_key_name(True))
        if not self.model._astra_hash:  # None if hash field is not exist
            self.model._astra_hash = {}
            self.model._astra_hash_exist = False
        else:
            self.model._astra_hash_exist = True

    def _convert_set(self, value):
        """ Check saved value before send to server """
        raise NotImplementedError('Subclasses must implement _convert_set')

    def _convert_get(self, value):
        """ Convert server answer to user type """
        raise NotImplementedError('Subclasses must implement _convert_get')

    def remove(self):
        """
        调用了 Redis.hdel() 删除哈希表中的一个指定字段
        """
        self.db.hdel(self.get_key_name(True), self.name)
        self.model._astra_hash_exist = None  # Need to verify again

    def force_check_hash_exists(self):
        """
        Redis.key.exists()  用于检测给定的key是否存在 1-存在  0-不存在
        """
        self.model._astra_hash_exist = bool(self.db.exists(
            self.get_key_name(True)))


# Implements for three types of lists
class BaseCollection(ForeignObjectValidatorMixin, ModelField):
    """
    field_type_name 赋值为 空字符串

    """
    field_type_name = ''
    _allowed_redis_methods = ()
    _single_object_answered_redis_methods = ()
    _list_answered_redis_methods = ()

    # Other methods will be answered directly

    def obtain(self):
        """
        返回： 对象本身
        """
        return self  # for delegate to __getattr__ on this class

    def assign(self, value):
        """
        ?? 为什么 value 为 None时，需要调用self.remove()方法
        """
        if value is None:
            self.remove()
        else:
            raise ValueError('Collections fields is not possible '
                             'assign directly')

    def __getattr__(self, item):
        """
        需要查看 self._allow_redis_methods 的值

        如果 item 不在 self._allow_redis_methods 元组中，说明 item 不是list set类型的method

        该 类方法里包含 两个 函数
        """
        if item not in self._allowed_redis_methods:
            return super(BaseCollection, self).__getattr__(item)

        original_command = getattr(self.db, item)
        current_key = self.get_key_name()

        from astra import model
        """
        传入的 value ?
        
        需要测试不同类型的value传入后  函数的表现
        看起来是一个递归方法
        """
        def modify_arg(value):
            # Helper could modify your args
            if isinstance(value, model.Model):
                return value.pk
            elif isinstance(value, (dt.datetime, dt.date,)):
                return int(value.strftime('%s'))
            elif isinstance(value, dict):
                # Scan dict and replace datetime values to timestamp. See .zadd
                new_dict = {}
                for k, v in value.items():
                    new_key = modify_arg(k)
                    new_dict[new_key] = modify_arg(v)
                return new_dict
            else:
                return value

        def _method_wrapper(*args, **kwargs):
            # Scan passed args and convert to pk if passed models
            new_args = [current_key]
            new_kwargs = dict()
            for v in args:
                new_args.append(modify_arg(v))
            new_kwargs = modify_arg(kwargs)

            # Call original method on the database
            answer = original_command(*new_args, **new_kwargs)

            # Wrap to model
            if item in self._single_object_answered_redis_methods:
                return None if not answer else self._to(answer)

            if item in self._list_answered_redis_methods:
                wrapper_answer = []
                for pk in answer:
                    if not pk:
                        wrapper_answer.append(None)
                    else:
                        if isinstance(pk, tuple) and len(pk) > 0:
                            wrapper_answer.append((self._to(pk[0]), pk[1]))
                        else:
                            wrapper_answer.append(self._to(pk))

                return wrapper_answer
            return answer  # Direct answer

        return _method_wrapper
