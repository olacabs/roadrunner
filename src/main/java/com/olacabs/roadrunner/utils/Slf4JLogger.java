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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.LoggerFactory;
import org.slf4j.Marker;


public final class Slf4JLogger implements Logger {

    @Override
    public void trace(long interval, String msg) {
        if ( shouldLog(interval) ) l.trace(msg);

    }
    @Override
    public void trace(long interval, String format, Object arg) {
        if ( shouldLog(interval) ) l.trace(format, arg);
    }
    @Override
    public void trace(long interval, String format, Object arg1, Object arg2) {
        if ( shouldLog(interval) ) l.trace(format, arg1, arg2);
    }
    @Override
    public void trace(long interval, String format, Object... arguments) {
        if ( shouldLog(interval) ) l.trace(format, arguments);
    }
    @Override
    public void trace(long interval, String msg, Throwable t) {
        if ( shouldLog(interval) ) l.trace(msg, t);
    }
    @Override
    public void trace(long interval, Marker marker, String msg) {
        if ( shouldLog(interval) ) l.trace(msg, marker, msg);
    }
    @Override
    public void trace(long interval, Marker marker, String format, Object arg) {
        if ( shouldLog(interval) ) l.trace(marker, format, arg);
    }
    @Override
    public void trace(long interval, Marker marker, String format, Object arg1, Object arg2) {
        if ( shouldLog(interval) ) l.trace(marker, format, arg1, arg2);
    }
    @Override
    public void trace(long interval, Marker marker, String format, Object... argArray) {
        if ( shouldLog(interval) ) l.trace(marker, format, argArray);
    }
    @Override
    public void trace(long interval, Marker marker, String msg, Throwable t) {
        if ( shouldLog(interval) ) l.trace(marker, msg, t);
    }
    @Override
    public void debug(long interval, String msg) {
        if ( shouldLog(interval) ) l.debug(msg);
    }
    @Override
    public void debug(long interval, String format, Object arg) {
        if ( shouldLog(interval) ) l.debug(format, arg);
    }
    @Override
    public void debug(long interval, String format, Object arg1, Object arg2) {
        if ( shouldLog(interval) ) l.debug(format, arg1, arg2);
    }
    @Override
    public void debug(long interval, String format, Object... arguments) {
        if ( shouldLog(interval) ) l.debug(format, arguments);
    }
    @Override
    public void debug(long interval, String msg, Throwable t) {
        if ( shouldLog(interval) ) l.debug(msg, t);
    }
    @Override
    public void debug(long interval, Marker marker, String msg) {
        if ( shouldLog(interval) ) l.debug(marker, msg);
    }
    @Override
    public void debug(long interval, Marker marker, String format, Object arg) {
        if ( shouldLog(interval) ) l.debug(marker, format, arg);
    }
    @Override
    public void debug(long interval, Marker marker, String format, Object arg1, Object arg2) {
        if ( shouldLog(interval) ) l.debug(marker, format, arg1, arg2);
    }
    @Override
    public void debug(long interval, Marker marker, String format, Object... argArray) {
        if ( shouldLog(interval) ) l.debug(marker, format, argArray);
    }
    @Override
    public void debug(long interval, Marker marker, String msg, Throwable t) {
        if ( shouldLog(interval) ) l.debug(marker, msg, t);
    }
    @Override
    public void info(long interval, String msg) {
        if ( shouldLog(interval) ) l.info( msg);
    }
    @Override
    public void info(long interval, String format, Object arg) {
        if ( shouldLog(interval) ) l.info( format, arg);
    }
    @Override
    public void info(long interval, String format, Object arg1, Object arg2) {
        if ( shouldLog(interval) ) l.info( format, arg1, arg2);
    }
    @Override
    public void info(long interval, String format, Object... arguments) {
        if ( shouldLog(interval) ) l.info( format, arguments);
    }
    @Override
    public void info(long interval, String msg, Throwable t) {
        if ( shouldLog(interval) ) l.info( msg, t);
    }
    @Override
    public void info(long interval, Marker marker, String msg) {
        if ( shouldLog(interval) ) l.info(marker, msg);
    }
    @Override
    public void info(long interval, Marker marker, String format, Object arg) {
        if ( shouldLog(interval) ) l.info(marker, format, arg);
    }
    @Override
    public void info(long interval, Marker marker, String format, Object arg1, Object arg2) {
        if ( shouldLog(interval) ) l.info(marker, format, arg1, arg2);
    }
    @Override
    public void info(long interval, Marker marker, String format, Object... argArray) {
        if ( shouldLog(interval) ) l.info(marker, format, argArray);
    }
    @Override
    public void info(long interval, Marker marker, String msg, Throwable t) {
        if ( shouldLog(interval) ) l.info(marker, msg, t);
    }
    public void warn(long interval,  String msg) {
        if ( shouldLog(interval) ) l.warn(msg);
    }
    @Override
    public void warn(long interval, String format, Object arg) {
        if ( shouldLog(interval) ) l.warn(format, arg);
    }
    @Override
    public void warn(long interval, String format, Object arg1, Object arg2) {
        if ( shouldLog(interval) ) l.warn(format, arg1, arg2);
    }
    @Override
    public void warn(long interval, String format, Object... arguments) {
        if ( shouldLog(interval) ) l.warn(format, arguments);
    }
    @Override
    public void warn(long interval, String msg, Throwable t) {
        if ( shouldLog(interval) ) l.warn(msg, t);
    }
    @Override
    public void warn(long interval, Marker marker, String msg) {
        if ( shouldLog(interval) ) l.warn(marker, msg);
    }
    @Override
    public void warn(long interval, Marker marker, String format, Object arg) {
        if ( shouldLog(interval) ) l.warn(marker, format, arg);
    }
    @Override
    public void warn(long interval, Marker marker, String format, Object arg1, Object arg2) {
        if ( shouldLog(interval) ) l.warn(marker, format, arg1, arg2);
    }
    @Override
    public void warn(long interval, Marker marker, String format, Object... argArray) {
        if ( shouldLog(interval) ) l.warn(marker, format, argArray);
    }
    @Override
    public void warn(long interval, Marker marker, String msg, Throwable t) {
        if ( shouldLog(interval) ) l.warn(marker, msg, t);
    }
    @Override
    public void error(long interval, String msg) {
        if ( shouldLog(interval) ) l.error(msg);
    }
    @Override
    public void error(long interval, String format, Object arg) {
        if ( shouldLog(interval) ) l.error(format, arg);
    }
    @Override
    public void error(long interval, String format, Object arg1, Object arg2) {
        if ( shouldLog(interval) ) l.error(format, arg1, arg2);
    }
    @Override
    public void error(long interval, String format, Object... arguments) {
        if ( shouldLog(interval) ) l.error(format, arguments);
    }
    @Override
    public void error(long interval, String msg, Throwable t) {
        if ( shouldLog(interval) ) l.error(msg, t);
    }
    @Override
    public void error(long interval, Marker marker, String msg) {
        if ( shouldLog(interval) ) l.error(marker, msg);
    }
    @Override
    public void error(long interval, Marker marker, String format, Object arg) {
        if ( shouldLog(interval) ) l.error(marker, format, arg);
    }
    @Override
    public void error(long interval, Marker marker, String format, Object arg1, Object arg2) {
        if ( shouldLog(interval) ) l.error(marker, format, arg1, arg2);
    }
    @Override
    public void error(long interval, Marker marker, String format, Object... argArray) {
        if ( shouldLog(interval) ) l.error(marker, format, argArray);
    }
    @Override
    public void error(long interval, Marker marker, String msg, Throwable t) {
        if ( shouldLog(interval) ) l.error(marker, msg, t);
    }
    private static class TimeLastLogged {
        public TimeLastLogged(long lastlogTime) {
            this.lastlogTime = lastlogTime;
        }
        public long lastlogTime = -1;
    }
    org.slf4j.Logger l = null;
    private static final ThreadLocal<StringBuilder> msgMaker =
            new ThreadLocal<StringBuilder>() {
                @Override protected StringBuilder initialValue() {
                    return new StringBuilder();
                }
            };

    static final Map<String, TimeLastLogged> stopLogging =
            new ConcurrentHashMap<String, TimeLastLogged>(4096);

    public Slf4JLogger(Class<?> classz){
        l = LoggerFactory.getLogger(classz);
    }

    public Slf4JLogger(String classz){
        l = LoggerFactory.getLogger(classz);
    }
    @Override
    public final boolean isTraceEnabled() {
        return l.isTraceEnabled();
    }
    @Override
    public final  void trace(final String message, final Throwable t) {
        l.trace(message.toString(), t);
    }
    @Override
    public final void trace(final String message) {
        l.trace(message);
    }
    @Override
    public final void debug(final String message, final Throwable t) {
        l.debug(message, t);
    }
    @Override
    public final void debug(final String message) {
        l.debug(message);
    }
    @Override
    public final void error(final String message, final Throwable t) {
        l.error(message,t);
    }
    @Override
    public final void error(final String message) {
        l.error(message);
    }

    @Override
    public final void error(String format, Object arg1, Object arg2){
        l.error(format, arg1, arg2);
    }

    @Override
    public final void info(final String message, final Throwable t) {
        l.info(message,t);
    }
    @Override
    public final void info(final String message) {
        l.info(message);
    }
    @Override
    public boolean isDebugEnabled() {
        return l.isDebugEnabled();
    }
    @Override
    public boolean isInfoEnabled() {
        return l.isInfoEnabled();
    }
    @Override
    public final void warn(final String message, final Throwable t) {
        l.warn(message,t);
    }
    @Override
    public final void warn(final String message) {
        l.warn(message);
    }
    @Override
    public String getName() {
        return l.getName();
    }
    @Override
    public void trace(String format, Object arg) {
        l.trace(format, arg);
    }
    @Override
    public void trace(String format, Object arg1, Object arg2) {
        l.trace(format, arg1, arg2);
    }
    @Override
    public void trace(String format, Object... arguments) {
        l.trace(format, arguments);
    }
    @Override
    public boolean isTraceEnabled(Marker marker) {
        return 	l.isTraceEnabled(marker);
    }
    @Override
    public void trace(Marker marker, String msg) {
        l.trace(marker, msg);
    }
    @Override
    public void trace(Marker marker, String format, Object arg) {
        l.trace(marker, format, arg);
    }
    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
        l.trace(marker, format, arg1, arg2);
    }
    @Override
    public void trace(Marker marker, String format, Object... argArray) {
        l.trace(marker, format, argArray);
    }
    @Override
    public void trace(Marker marker, String msg, Throwable t) {
        l.trace(marker, msg, t);
    }
    @Override
    public void debug(String format, Object arg) {
        l.debug(format, arg);
    }
    @Override
    public void debug(String format, Object arg1, Object arg2) {
        l.debug(format, arg1, arg2);
    }
    @Override
    public void debug(String format, Object... arguments) {
        l.debug(format, arguments);
    }
    @Override
    public boolean isDebugEnabled(Marker marker) {
        return l.isDebugEnabled(marker);
    }
    @Override
    public void debug(Marker marker, String msg) {
        l.debug(marker, msg);
    }
    @Override
    public void debug(Marker marker, String format, Object arg) {
        l.debug(marker, format, arg);
    }
    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
        l.debug(marker, format, arg1, arg2);
    }
    @Override
    public void debug(Marker marker, String format, Object... arguments) {
        l.debug(marker, format, arguments);
    }
    @Override
    public void debug(Marker marker, String msg, Throwable t) {
        l.debug(marker, msg, t);
    }
    @Override
    public void info(String format, Object arg) {
        l.info(format, arg);
    }
    @Override
    public void info(String format, Object arg1, Object arg2) {
        l.info(format, arg1, arg2);
    }
    @Override
    public void info(String format, Object... arguments) {
        l.info(format, arguments);
    }
    @Override
    public boolean isInfoEnabled(Marker marker) {
        return 	l.isInfoEnabled(marker);
    }
    @Override
    public void info(Marker marker, String msg) {
        l.info(marker, msg);
    }
    @Override
    public void info(Marker marker, String format, Object arg) {
        l.info(marker, format, arg);
    }
    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
        l.info(marker, format, arg1, arg2);
    }
    @Override
    public void info(Marker marker, String format, Object... arguments) {
        l.info(marker, format, arguments);
    }
    @Override
    public void info(Marker marker, String msg, Throwable t) {
        l.info(marker, msg, t);
    }
    @Override
    public boolean isWarnEnabled() {
        return l.isWarnEnabled();
    }
    @Override
    public void warn(String format, Object arg) {
        l.warn(format, arg);
    }
    @Override
    public void warn(String format, Object... arguments) {
        l.warn(format, arguments);
    }
    @Override
    public void warn(String format, Object arg1, Object arg2) {
        l.warn(format, arg1, arg2);
    }
    @Override
    public boolean isWarnEnabled(Marker marker) {
        return l.isWarnEnabled(marker);
    }
    @Override
    public void warn(Marker marker, String msg) {
        l.warn(marker, msg);
    }
    @Override
    public void warn(Marker marker, String format, Object arg) {
        l.warn(marker, format, arg);
    }
    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
        l.warn(marker, format, arg1, arg2);
    }
    @Override
    public void warn(Marker marker, String format, Object... arguments) {
        l.warn(marker, format, arguments);
    }
    @Override
    public void warn(Marker marker, String msg, Throwable t) {
        l.warn(marker, msg, t);
    }
    @Override
    public boolean isErrorEnabled() {
        return l.isErrorEnabled();
    }
    @Override
    public void error(String format, Object arg) {
        l.error(format, arg);
    }
    @Override
    public void error(String format, Object... arguments) {
        l.error(format, arguments);
    }
    @Override
    public boolean isErrorEnabled(Marker marker) {
        return l.isErrorEnabled(marker);
    }
    @Override
    public void error(Marker marker, String msg) {
        l.error(marker, msg);
    }
    @Override
    public void error(Marker marker, String format, Object arg) {
        l.error(marker, format, arg);
    }
    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
        l.error(marker, format, arg1, arg2);
    }
    @Override
    public void error(Marker marker, String format, Object... arguments) {
        l.error(marker, format, arguments);
    }
    @Override
    public void error(Marker marker, String msg, Throwable t) {
        l.error(marker, msg, t);
    }



    @Override
    public void trace(String identifier, long interval , String msg) {
        if ( shouldLog(identifier, interval) ) l.trace(msg);

    }
    @Override
    public void trace(String identifier, long interval , String format, Object arg) {
        if ( shouldLog(identifier, interval) ) l.trace(format, arg);
    }
    @Override
    public void trace(String identifier, long interval , String format, Object arg1, Object arg2) {
        if ( shouldLog(identifier, interval) ) l.trace(format, arg1, arg2);
    }
    @Override
    public void trace(String identifier, long interval , String format, Object... arguments) {
        if ( shouldLog(identifier, interval) ) l.trace(format, arguments);
    }
    @Override
    public void trace(String identifier, long interval , String msg, Throwable t) {
        if ( shouldLog(identifier, interval) ) l.trace(msg, t);
    }
    @Override
    public void trace(String identifier, long interval , Marker marker, String msg) {
        if ( shouldLog(identifier, interval) ) l.trace(msg, marker, msg);
    }
    @Override
    public void trace(String identifier, long interval , Marker marker, String format, Object arg) {
        if ( shouldLog(identifier, interval) ) l.trace(marker, format, arg);
    }
    @Override
    public void trace(String identifier, long interval , Marker marker, String format, Object arg1, Object arg2) {
        if ( shouldLog(identifier, interval) ) l.trace(marker, format, arg1, arg2);
    }
    @Override
    public void trace(String identifier, long interval , Marker marker, String format, Object... argArray) {
        if ( shouldLog(identifier, interval) ) l.trace(marker, format, argArray);
    }
    @Override
    public void trace(String identifier, long interval , Marker marker, String msg, Throwable t) {
        if ( shouldLog(identifier, interval) ) l.trace(marker, msg, t);
    }
    @Override
    public void debug(String identifier, long interval , String msg) {
        if ( shouldLog(identifier, interval) ) l.debug(msg);
    }
    @Override
    public void debug(String identifier, long interval , String format, Object arg) {
        if ( shouldLog(identifier, interval) ) l.debug(format, arg);
    }
    @Override
    public void debug(String identifier, long interval , String format, Object arg1, Object arg2) {
        if ( shouldLog(identifier, interval) ) l.debug(format, arg1, arg2);
    }
    @Override
    public void debug(String identifier, long interval , String format, Object... arguments) {
        if ( shouldLog(identifier, interval) ) l.debug(format, arguments);
    }
    @Override
    public void debug(String identifier, long interval , String msg, Throwable t) {
        if ( shouldLog(identifier, interval) ) l.debug(msg, t);
    }
    @Override
    public void debug(String identifier, long interval , Marker marker, String msg) {
        if ( shouldLog(identifier, interval) ) l.debug(marker, msg);
    }
    @Override
    public void debug(String identifier, long interval , Marker marker, String format, Object arg) {
        if ( shouldLog(identifier, interval) ) l.debug(marker, format, arg);
    }
    @Override
    public void debug(String identifier, long interval , Marker marker, String format, Object arg1, Object arg2) {
        if ( shouldLog(identifier, interval) ) l.debug(marker, format, arg1, arg2);
    }
    @Override
    public void debug(String identifier, long interval , Marker marker, String format, Object... argArray) {
        if ( shouldLog(identifier, interval) ) l.debug(marker, format, argArray);
    }
    @Override
    public void debug(String identifier, long interval , Marker marker, String msg, Throwable t) {
        if ( shouldLog(identifier, interval) ) l.debug(marker, msg, t);
    }
    @Override
    public void info(String identifier, long interval , String msg) {
        if ( shouldLog(identifier, interval) ) l.info( msg);
    }
    @Override
    public void info(String identifier, long interval , String format, Object arg) {
        if ( shouldLog(identifier, interval) ) l.info( format, arg);
    }
    @Override
    public void info(String identifier, long interval , String format, Object arg1, Object arg2) {
        if ( shouldLog(identifier, interval) ) l.info( format, arg1, arg2);
    }
    @Override
    public void info(String identifier, long interval , String format, Object... arguments) {
        if ( shouldLog(identifier, interval) ) l.info( format, arguments);
    }
    @Override
    public void info(String identifier, long interval , String msg, Throwable t) {
        if ( shouldLog(identifier, interval) ) l.info( msg, t);
    }
    @Override
    public void info(String identifier, long interval , Marker marker, String msg) {
        if ( shouldLog(identifier, interval) ) l.info(marker, msg);
    }
    @Override
    public void info(String identifier, long interval , Marker marker, String format, Object arg) {
        if ( shouldLog(identifier, interval) ) l.info(marker, format, arg);
    }
    @Override
    public void info(String identifier, long interval , Marker marker, String format, Object arg1, Object arg2) {
        if ( shouldLog(identifier, interval) ) l.info(marker, format, arg1, arg2);
    }
    @Override
    public void info(String identifier, long interval , Marker marker, String format, Object... argArray) {
        if ( shouldLog(identifier, interval) ) l.info(marker, format, argArray);
    }
    @Override
    public void info(String identifier, long interval , Marker marker, String msg, Throwable t) {
        if ( shouldLog(identifier, interval) ) l.info(marker, msg, t);
    }
    public void warn(String identifier, long interval ,  String msg) {
        if ( shouldLog(identifier, interval) ) l.warn(msg);
    }
    @Override
    public void warn(String identifier, long interval , String format, Object arg) {
        if ( shouldLog(identifier, interval) ) l.warn(format, arg);
    }
    @Override
    public void warn(String identifier, long interval , String format, Object arg1, Object arg2) {
        if ( shouldLog(identifier, interval) ) l.warn(format, arg1, arg2);
    }
    @Override
    public void warn(String identifier, long interval , String format, Object... arguments) {
        if ( shouldLog(identifier, interval) ) l.warn(format, arguments);
    }
    @Override
    public void warn(String identifier, long interval , String msg, Throwable t) {
        if ( shouldLog(identifier, interval) ) l.warn(msg, t);
    }
    @Override
    public void warn(String identifier, long interval , Marker marker, String msg) {
        if ( shouldLog(identifier, interval) ) l.warn(marker, msg);
    }
    @Override
    public void warn(String identifier, long interval , Marker marker, String format, Object arg) {
        if ( shouldLog(identifier, interval) ) l.warn(marker, format, arg);
    }
    @Override
    public void warn(String identifier, long interval , Marker marker, String format, Object arg1, Object arg2) {
        if ( shouldLog(identifier, interval) ) l.warn(marker, format, arg1, arg2);
    }
    @Override
    public void warn(String identifier, long interval , Marker marker, String format, Object... argArray) {
        if ( shouldLog(identifier, interval) ) l.warn(marker, format, argArray);
    }
    @Override
    public void warn(String identifier, long interval , Marker marker, String msg, Throwable t) {
        if ( shouldLog(identifier, interval) ) l.warn(marker, msg, t);
    }
    @Override
    public void error(String identifier, long interval , String msg) {
        if ( shouldLog(identifier, interval) ) l.error(msg);
    }
    @Override
    public void error(String identifier, long interval , String format, Object arg) {
        if ( shouldLog(identifier, interval) ) l.error(format, arg);
    }
    @Override
    public void error(String identifier, long interval , String format, Object arg1, Object arg2) {
        if ( shouldLog(identifier, interval) ) l.error(format, arg1, arg2);
    }
    @Override
    public void error(String identifier, long interval , String format, Object... arguments) {
        if ( shouldLog(identifier, interval) ) l.error(format, arguments);
    }
    @Override
    public void error(String identifier, long interval , String msg, Throwable t) {
        if ( shouldLog(identifier, interval) ) l.error(msg, t);
    }
    @Override
    public void error(String identifier, long interval , Marker marker, String msg) {
        if ( shouldLog(identifier, interval) ) l.error(marker, msg);
    }
    @Override
    public void error(String identifier, long interval , Marker marker, String format, Object arg) {
        if ( shouldLog(identifier, interval) ) l.error(marker, format, arg);
    }
    @Override
    public void error(String identifier, long interval , Marker marker, String format, Object arg1, Object arg2) {
        if ( shouldLog(identifier, interval) ) l.error(marker, format, arg1, arg2);
    }
    @Override
    public void error(String identifier, long interval , Marker marker, String format, Object... argArray) {
        if ( shouldLog(identifier, interval) ) l.error(marker, format, argArray);
    }
    @Override
    public void error(String identifier, long interval , Marker marker, String msg, Throwable t) {
        if ( shouldLog(identifier, interval) ) l.error(marker, msg, t);
    }


    private final boolean shouldLog(final long interval) {

        if ( interval > 0 ) {

            StringBuilder msgBuilder = msgMaker.get();
            msgBuilder.setLength(0);

            StackTraceElement stackTraceElement = Thread.currentThread().getStackTrace()[3];
            String where = msgBuilder.append(stackTraceElement.getClassName()).append(
                    stackTraceElement.getMethodName()).append( stackTraceElement.getLineNumber() ).toString();

            TimeLastLogged tll = stopLogging.get(where);
            long curTime = System.currentTimeMillis();
            if ( null == tll) {
                tll = new TimeLastLogged(curTime);
                stopLogging.put(where, tll);
            } else {
                if  ( (tll.lastlogTime + interval) > curTime ) return false;
                else tll.lastlogTime = curTime;
            }
        }

        return true;
    }
    private final boolean shouldLog(String where, final long interval) {

        if ( interval > 0 ) {
            TimeLastLogged tll = stopLogging.get(where);
            long curTime = System.currentTimeMillis();
            if ( null == tll) {
                tll = new TimeLastLogged(curTime);
                stopLogging.put(where, tll);
            } else {
                if  ( (tll.lastlogTime + interval) > curTime ) return false;
                else tll.lastlogTime = curTime;
            }
        }

        return true;
    }

}
