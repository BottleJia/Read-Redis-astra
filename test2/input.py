# from astra import models
#
# class UserObject(models.Model):
#     def
#
#
#


class MetaDemo(type):
    pass


class Demo(metaclass=MetaDemo):

    pass

d = Demo()
print(d.__class__.__metaclass__)
