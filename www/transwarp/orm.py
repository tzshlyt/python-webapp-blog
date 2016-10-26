#!/usr/bin/env python
# coding: utf-8

import time
import logging
import db

_triggers = frozenset(['pre_insert', 'pre_update', 'pre_delete'])


def _gen_sql(table_name, mappings):
    pk = None
    sql = ['-- generating SQL for %s:' %
           table_name, 'create table `%s` (' % table_name]
    for f in sorted(mappings.values(), lambda x, y: cmp(x._order, y._order)):
        if not hasattr(f, 'ddl'):
            raise StandardError('no ddl in field "%s".' % f)
        ddl = f.ddl
        nullable = f.nullable
        if f.primary_key:
            pk = f.name
        sql.append(nullable and ' `%s` %s,' %
                   (f.name, ddl) or ' `%s` %s not null,' % (f.name, ddl))
    sql.append(' primary key(`%s`)' % pk)
    sql.append(');')

    print '---sql: '
    print '\n'.join(sql)
    return '\n'.join(sql)


class Field(object):

    _count = 0

    def __init__(self, **kw):
        self.name = kw.get('name', None)
        self._default = kw.get('default', None)
        self.primary_key = kw.get('primary_key', False)
        self.nullable = kw.get('nullable', False)
        self.updatable = kw.get('updatable', True)
        self.insertable = kw.get('insertable', True)
        self.ddl = kw.get('ddl', '')
        self._order = Field._count
        Field._count += 1

    @property
    def default(self):
        d = self._default
        print '---default: '
        print d
        return d() if callable(d) else d

    def __str__(self):
        s = ['<%s:%s,%s,default(%s),' % (
            self.__class__.__name__, self.name, self.ddl, self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)


class StringField(Field):

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = ''
        if 'ddl' not in kw:
            kw['ddl'] = 'varchar(255)'
        super(StringField, self).__init__(**kw)


class IntegerField(Field):

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = 0
        if 'ddl' not in kw:
            kw['ddl'] = 'bigint'
        super(IntegerField, self).__init__(**kw)


class FloatField(Field):

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = 0.0
        if 'ddl' not in kw:
            kw['ddl'] = 'real'
        super(FloatField, self).__init__(**kw)


class BooleanField(Field):

    def __init__(self, **kw):
        if not 'default' in kw:
            kw['default'] = False
        if not 'ddl' in kw:
            kw['ddl'] = 'bool'
        super(BooleanField, self).__init__(**kw)


class TextField(Field):

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = ''
        if 'ddl' not in kw:
            kw['ddl'] = 'text'
        super(TextField, self).__init__(**kw)


class BlobField(Field):

    def __init__(self, **kw):
        if 'default' not in kw:
            kw['default'] = ''
        if 'ddl' not in kw:
            kw['ddl'] = 'blob'
        super(BlobField, self).__init__(**kw)


class VersionField(Field):

    def __init__(self, name=None):
        super(VersionField, self).__init__(name=name, default=0, ddl='bigint')


class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):

        print '---name: ' + name

        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)

        if not hasattr(cls, 'subclasses'):
            cls.subclasses = {}
        if not name in cls.subclasses:
            cls.subclasses[name] = name
        else:
            logging.warning('Redefine class: %s' % name)

        logging.info('Scan ORMapping %s...' % name)
        mappings = dict()
        primary_key = None
        for k, v in attrs.iteritems():
            if isinstance(v, Field):
                if not v.name:
                    v.name = k
                logging.info('[MAPPING] Found mapping: %s => %s' % (k, v))

                if v.primary_key:
                    if primary_key:
                        raise TypeError(
                            'Cannot define more than 1 primary key in class %s' % name)
                    if v.updatable:
                        logging.warning(
                            'NOTE: change primary key to non-updatable.')
                        v.updatable = False
                    if v.nullable:
                        logging.warning(
                            'NOTE: change primary key to non-nullable.')
                        v.nullable = False
                    primary_key = v

                mappings[k] = v

        print '---mappings: '
        print mappings

        if not primary_key:
            raise TypeError('Primary key not defined in class: %s' % name)

        print '---attrs: '
        print attrs

        for k in mappings.iterkeys():
            attrs.pop(k)
        if not '__table__' in attrs:
            attrs['__table__'] = name.lower()

        attrs['__mappings__'] = mappings
        attrs['__primary_key__'] = primary_key
        attrs['__sql__'] = lambda self: _gen_sql(attrs['__table__'], mappings)
        for trigger in _triggers:
            if not trigger in attrs:
                attrs[trigger] = None

        _gen_sql(attrs['__table__'], mappings)
        print '---attrs change: '
        print attrs

        return type.__new__(cls, name, bases, attrs)


class Model(dict):
    __metaclass__ = ModelMetaclass

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    @classmethod
    def get(cls, pk):
        d = db.select_one('select * from %s where %s=?' %
                          (cls.__table__, cls.__primary_key__.name), pk)
        return cls(**d) if d else None

    @classmethod
    def find_first(cls, where, *args):
        print where
        print args
        d = db.select_one('select * from %s %s' %
                          (cls.__table__, where), *args)
        return cls(**d) if d else None

    @classmethod
    def find_all(cls, *args):
        L = db.select('select * from `%s`' % cls.__table__)
        return [cls(**d) for d in L]

    @classmethod
    def find_by(cls, where, *args):
        L = db.select('select * from `%s` %s' % (cls.__table__, where), *args)
        return [cls(**d) for d in L]

    @classmethod
    def count_all(cls):
        return db.select_int('select count(`%s`) from `%s`' % (cls.__primary_key__.name, cls.__table__))

    @classmethod
    def count_by(cls, where, *args):
        return db.select_int('select count(`%s`) from `%s` %s' % (cls.__primary_key__.name, cls.__table__, where), *args)

    def update(self):
        self.pre_update and self.pre_update()
        L = []
        args = []
        for k, v in self.__mappings__.iteritems():
            if v.updatable:
                if hasattr(self, k):
                    arg = getattr(self, k)
                else:
                    arg = v.default
                    setattr(self, k, arg)
                L.append('`%s`=?' % k)
                args.append(arg)
        pk = self.__primary_key__.name
        args.append(getattr(self, pk))
        db.update('update `%s` set %s where %s=?' %
                  (self.__table__, ','.join(L), pk), *args)
        return self

    def delete(self):
        self.pre_delete and self.pre_delete()
        pk = self.__primary_key__.name
        args = (getattr(self, pk), )
        db.update('delete from `%s` where `%s`=?' %
                  (self.__table__, pk), *args)
        return self

    def insert(self):
        self.pre_insert and self.pre_insert()
        params = {}
        for k, v in self.__mappings__.iteritems():

            if v.insertable:
                if not hasattr(self, k):
                    setattr(self, k, v.default)
                params[v.name] = getattr(self, k)

        print params
        print self.__table__
        db.insert('%s' % self.__table__, **params)
        return self

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    class User(Model):
        id = IntegerField(primary_key=True)
        name = StringField()
        email = StringField(updatable=False)
        passwd = StringField(default=lambda: '******')
        last_modified = FloatField()

        def pre_insert(self):
            self.last_modified = time.time()

    db.create_engine('user', 'www-data', 'blogdb')

    # u = User(id=200, name='Michael', email='orm@db.org',passwd='123abc')
    # u.insert()

    print '---delet(): '
    g = User.get(2008)
    if g:
        print g.name
        g.delete()
    else:
        print 'not exist'

    print '---update(): '
    f = User.get(20030)
    print f.name
    print f.email
    f.name = 'jfkdjfj'
    f.passwd = '11111'
    f.email = 'change@db.org'
    r = f.update()

    print '---get(): '
    print User.get(2000)

    print '---find_first(): '
    print User.find_first('where name=?', 'Michael')

    print '---find_all(): '
    print len(User.find_all())

    print '---count_all(): '
    print User.count_all()

    # print '---count_by(): '
 #    print User.count_by('while')
