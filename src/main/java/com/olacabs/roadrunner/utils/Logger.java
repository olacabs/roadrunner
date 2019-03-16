//************************************************************
// Copyright 2019 ANI Technologies Pvt. Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//************************************************************
package com.olacabs.roadrunner.utils;

import org.slf4j.Marker;

public interface Logger extends org.slf4j.Logger
{
    static final int TRACE = 0;
    static final int DEBUG = 1;
    static final int INFO = 2;
    static final int WARN = 3;
    static final int ERROR = 4;
    static final int FATAL = 5;


    public void trace(long interval,  String msg);
    public void trace(long interval, String format, Object arg);
    public void trace(long interval, String format, Object arg1, Object arg2);
    public void trace(long interval, String format, Object... arguments);
    public void trace(long interval, String msg, Throwable t);
    public void trace(long interval, Marker marker, String msg);
    public void trace(long interval, Marker marker, String format, Object arg);
    public void trace(long interval, Marker marker, String format, Object arg1, Object arg2);
    public void trace(long interval, Marker marker, String format, Object... argArray);
    public void trace(long interval, Marker marker, String msg, Throwable t);

    public void debug(long interval,  String msg);
    public void debug(long interval, String format, Object arg);
    public void debug(long interval, String format, Object arg1, Object arg2);
    public void debug(long interval, String format, Object... arguments);
    public void debug(long interval, String msg, Throwable t);
    public void debug(long interval, Marker marker, String msg);
    public void debug(long interval, Marker marker, String format, Object arg);
    public void debug(long interval, Marker marker, String format, Object arg1, Object arg2);
    public void debug(long interval, Marker marker, String format, Object... argArray);
    public void debug(long interval, Marker marker, String msg, Throwable t);

    public void info(long interval,  String msg);
    public void info(long interval, String format, Object arg);
    public void info(long interval, String format, Object arg1, Object arg2);
    public void info(long interval, String format, Object... arguments);
    public void info(long interval, String msg, Throwable t);
    public void info(long interval, Marker marker, String msg);
    public void info(long interval, Marker marker, String format, Object arg);
    public void info(long interval, Marker marker, String format, Object arg1, Object arg2);
    public void info(long interval, Marker marker, String format, Object... argArray);
    public void info(long interval, Marker marker, String msg, Throwable t);
    public void warn(long interval,  String msg);
    public void warn(long interval, String format, Object arg);
    public void warn(long interval, String format, Object arg1, Object arg2);
    public void warn(long interval, String format, Object... arguments);
    public void warn(long interval, String msg, Throwable t);
    public void warn(long interval, Marker marker, String msg);
    public void warn(long interval, Marker marker, String format, Object arg);
    public void warn(long interval, Marker marker, String format, Object arg1, Object arg2);
    public void warn(long interval, Marker marker, String format, Object... argArray);
    public void warn(long interval, Marker marker, String msg, Throwable t);

    public void error(long interval,  String msg);
    public void error(long interval, String format, Object arg);
    public void error(long interval, String format, Object arg1, Object arg2);
    public void error(long interval, String format, Object... arguments);
    public void error(long interval, String msg, Throwable t);
    public void error(long interval, Marker marker, String msg);
    public void error(long interval, Marker marker, String format, Object arg);
    public void error(long interval, Marker marker, String format, Object arg1, Object arg2);
    public void error(long interval, Marker marker, String format, Object... argArray);
    public void error(long interval, Marker marker, String msg, Throwable t);

    public void trace(String identifier, long interval ,  String msg);
    public void trace(String identifier, long interval , String format, Object arg);
    public void trace(String identifier, long interval , String format, Object arg1, Object arg2);
    public void trace(String identifier, long interval , String format, Object... arguments);
    public void trace(String identifier, long interval , String msg, Throwable t);
    public void trace(String identifier, long interval , Marker marker, String msg);
    public void trace(String identifier, long interval , Marker marker, String format, Object arg);
    public void trace(String identifier, long interval , Marker marker, String format, Object arg1, Object arg2);
    public void trace(String identifier, long interval , Marker marker, String format, Object... argArray);
    public void trace(String identifier, long interval , Marker marker, String msg, Throwable t);

    public void debug(String identifier, long interval ,  String msg);
    public void debug(String identifier, long interval , String format, Object arg);
    public void debug(String identifier, long interval , String format, Object arg1, Object arg2);
    public void debug(String identifier, long interval , String format, Object... arguments);
    public void debug(String identifier, long interval , String msg, Throwable t);
    public void debug(String identifier, long interval , Marker marker, String msg);
    public void debug(String identifier, long interval , Marker marker, String format, Object arg);
    public void debug(String identifier, long interval , Marker marker, String format, Object arg1, Object arg2);
    public void debug(String identifier, long interval , Marker marker, String format, Object... argArray);
    public void debug(String identifier, long interval , Marker marker, String msg, Throwable t);

    public void info(String identifier, long interval ,  String msg);
    public void info(String identifier, long interval , String format, Object arg);
    public void info(String identifier, long interval , String format, Object arg1, Object arg2);
    public void info(String identifier, long interval , String format, Object... arguments);
    public void info(String identifier, long interval , String msg, Throwable t);
    public void info(String identifier, long interval , Marker marker, String msg);
    public void info(String identifier, long interval , Marker marker, String format, Object arg);
    public void info(String identifier, long interval , Marker marker, String format, Object arg1, Object arg2);
    public void info(String identifier, long interval , Marker marker, String format, Object... argArray);
    public void info(String identifier, long interval , Marker marker, String msg, Throwable t);
    public void warn(String identifier, long interval ,  String msg);
    public void warn(String identifier, long interval , String format, Object arg);
    public void warn(String identifier, long interval , String format, Object arg1, Object arg2);
    public void warn(String identifier, long interval , String format, Object... arguments);
    public void warn(String identifier, long interval , String msg, Throwable t);
    public void warn(String identifier, long interval , Marker marker, String msg);
    public void warn(String identifier, long interval , Marker marker, String format, Object arg);
    public void warn(String identifier, long interval , Marker marker, String format, Object arg1, Object arg2);
    public void warn(String identifier, long interval , Marker marker, String format, Object... argArray);
    public void warn(String identifier, long interval , Marker marker, String msg, Throwable t);

    public void error(String identifier, long interval ,  String msg);
    public void error(String identifier, long interval , String format, Object arg);
    public void error(String identifier, long interval , String format, Object arg1, Object arg2);
    public void error(String identifier, long interval , String format, Object... arguments);
    public void error(String identifier, long interval , String msg, Throwable t);
    public void error(String identifier, long interval , Marker marker, String msg);
    public void error(String identifier, long interval , Marker marker, String format, Object arg);
    public void error(String identifier, long interval , Marker marker, String format, Object arg1, Object arg2);
    public void error(String identifier, long interval , Marker marker, String format, Object... argArray);
    public void error(String identifier, long interval , Marker marker, String msg, Throwable t);
}