#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

from transwarp.web import get, view
from models import User, Blog, Comment


# @view('test_users.html')
# @get('/')
# def test_users():
#     users = User.find_all()
#     print ">>>>>>>>> urls   "
#     print users
#     return dict(users=users)

@view('blogs.html')
@get('/')
def index():
    blogs = Blog.find_all()
    user = User.find_first('where email=?', 'admin@example.com')
    return dict(blogs=blogs, user=user)
