/*
 * Copyright (c) 2011-2019 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.core.http.impl;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.http.impl.headers.HeadersAdaptor;
import io.vertx.core.internal.buffer.BufferInternal;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;
import io.vertx.core.streams.WriteStream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class HttpClientResponseImpl implements HttpClientResponse  {

  private static final Logger log = LoggerFactory.getLogger(HttpClientResponseImpl.class);

  private final HttpVersion version;
  private final int statusCode;
  private final String statusMessage;
  private final HttpClientRequestBase request;
  private final HttpClientStream stream;

  private final HttpEventHandler eventHandler;
  private Handler<HttpFrame> customFrameHandler;
  private Handler<StreamPriority> priorityHandler;

  // Cache these for performance
  private final MultiMap headers;
  private MultiMap trailers;
  private List<String> cookies;
  private NetSocket netSocket;

  private final Promise<Void> completion;

  HttpClientResponseImpl(HttpClientRequestBase request, HttpVersion version, HttpClientStream stream, int statusCode, String statusMessage, MultiMap headers) {
    this.version = version;
    this.statusCode = statusCode;
    this.statusMessage = statusMessage;
    this.request = request;
    this.stream = stream;
    this.completion = request.context.promise();
    this.headers = headers;

    this.eventHandler = new HttpEventHandler(request.context);
  }

//  private HttpEventHandler eventHandler(boolean create) {
//    if (eventHandler == null && create) {
//      eventHandler = new HttpEventHandler(request.context);
//    }
//    return eventHandler;
//  }

  @Override
  public HttpClientRequestBase request() {
    return request;
  }

  @Override
  public NetSocket netSocket() {
    if (netSocket == null) {
      netSocket = HttpNetSocket.netSocket(stream, request.context, this, new WriteStream<>() {
        @Override
        public WriteStream<Buffer> exceptionHandler(@Nullable Handler<Throwable> handler) {
          stream.exceptionHandler(handler);
          return this;
        }
        @Override
        public Future<Void> write(Buffer data) {
          return stream.writeChunk(data, false);
        }
        @Override
        public Future<Void> end(Buffer data) {
          return stream.writeChunk(data, true);
        }
        @Override
        public Future<Void> end() {
          return stream.writeChunk(BufferInternal.buffer(Unpooled.EMPTY_BUFFER), true);
        }
        @Override
        public WriteStream<Buffer> setWriteQueueMaxSize(int maxSize) {
          stream.setWriteQueueMaxSize(maxSize);
          return this;
        }
        @Override
        public boolean writeQueueFull() {
          return !stream.isWritable();
        }
        @Override
        public WriteStream<Buffer> drainHandler(@Nullable Handler<Void> handler) {
          stream.drainHandler(handler);
          return this;
        }
      });
    }
    return netSocket;
  }

  @Override
  public HttpVersion version() {
    return version;
  }

  @Override
  public int statusCode() {
    return statusCode;
  }

  @Override
  public String statusMessage() {
    return statusMessage;
  }

  @Override
  public MultiMap headers() {
    return headers;
  }

  @Override
  public String getHeader(String headerName) {
    return headers.get(headerName);
  }

  @Override
  public String getHeader(CharSequence headerName) {
    return headers.get(headerName);
  }

  @Override
  public MultiMap trailers() {
    boolean onContext = request.context.isRunningOnContext();
    return onContext ? onContextTrailers() : request.context.<MultiMap>future(h -> h.complete(onContextTrailers())).await();
  }

  private MultiMap onContextTrailers() {
    if (trailers == null) {
      trailers = new HeadersAdaptor(new DefaultHttpHeaders());
    }
    return trailers;
  }

  private <R> R onEventLoop(Supplier<R> supplier) {
    if (request.context.eventLoop().inThread()) {
      return supplier.get();
    } else {
      return request.context.<R>future(p -> p.complete(supplier.get())).await();
    }
  }

  private <P> void onEventLoop(P value, Handler<P> handler) {
    if (request.context.eventLoop().inThread()) {
      handler.handle(value);
    } else {
      request.context.eventLoop().execute(() -> handler.handle(value));
    }
  }

  @Override
  public String getTrailer(String trailerName) {
    MultiMap trailers = onEventLoop(this::onEventLoopTrailers);
    return trailers != null ? trailers.get(trailerName) : null;
  }

  private MultiMap onEventLoopTrailers() {
    return this.trailers;
  }

  @Override
  public List<String> cookies() {
    return onEventLoop(this::onEventLoopCookies);
  }

  private List<String> onEventLoopCookies() {
    if (cookies == null) {
      cookies = new ArrayList<>();
      cookies.addAll(headers().getAll(HttpHeaders.SET_COOKIE));
      if (trailers != null) {
        cookies.addAll(trailers.getAll(HttpHeaders.SET_COOKIE));
      }
    }
    return cookies;
  }

  /** Must be called within a {@code synchronized (conn)} block. */
  private void checkEnded() {
    if (completion.future().isComplete()) {
      throw new IllegalStateException("Response already ended");
    }
  }

  @Override
  public HttpClientResponse handler(Handler<Buffer> handler) {
    if (handler != null) {
      checkEnded();
    }
    onEventLoop(handler, this::onEventLoopSetHandler);
    return this;
  }

  private void onEventLoopSetHandler(Handler<Buffer> handler) {
    eventHandler.chunkHandler(handler);
  }

  @Override
  public HttpClientResponse endHandler(Handler<Void> handler) {
    if (handler != null) {
      checkEnded();
    }
    onEventLoop(handler, this::onEventLoopSetEndHandler);
    return this;
  }

  private void onEventLoopSetEndHandler(Handler<Void> handler) {
    eventHandler.endHandler(handler);
  }

  @Override
  public HttpClientResponse exceptionHandler(Handler<Throwable> handler) {
    if (handler != null) {
      checkEnded();
    }
    onEventLoop(handler, this::onEventLoopSetExceptionHandler);
    return this;
  }

  private void onEventLoopSetExceptionHandler(Handler<Throwable> handler) {
    eventHandler.exceptionHandler(handler);
  }

  @Override
  public HttpClientResponse pause() {
    stream.pause();
    return this;
  }

  @Override
  public HttpClientResponse resume() {
    return fetch(Long.MAX_VALUE);
  }

  @Override
  public HttpClientResponse fetch(long amount) {
    stream.fetch(amount);
    return this;
  }

  @Override
  public HttpClientResponse customFrameHandler(Handler<HttpFrame> handler) {
    if (handler != null) {
      checkEnded();
    }
    onEventLoop(handler, this::onContextSetCustomFrameHandler);
    return this;
  }

  private void onContextSetCustomFrameHandler(Handler<HttpFrame> handler) {
    customFrameHandler = handler;
  }

  void handleUnknownFrame(HttpFrame frame) {
    onEventLoop(frame, this::onContextHandleUnknownFrame);
  }

  private void onContextHandleUnknownFrame(HttpFrame frame) {
    Handler<HttpFrame> handler = customFrameHandler;
    if (handler != null) {
      handler.handle(frame);
    }
  }

  void handleChunk(Buffer data) {
    onEventLoop(data, this::onContextHandleChunk);
  }

  private void onContextHandleChunk(Buffer data) {
    eventHandler.handleChunk(data);
  }

  void handleTrailers(MultiMap trailers) {
    onEventLoop(trailers, this::onContextHandleTrailers);
  }

  private void onContextHandleTrailers(MultiMap trailers) {
    if (this.trailers == null) {
      this.trailers = trailers;
    } else if (this.trailers != trailers) {
      this.trailers.setAll(trailers);
    }

    if (!completion.tryComplete()) {
      return;
    }

    eventHandler.handleEnd();
  }

  void handleException(Throwable e) {
    onEventLoop(e, this::onEventLoopHandleException);
  }

  private void onEventLoopHandleException(Throwable e) {
    if (!completion.tryFail(e)) {
      return;
    }

    if (!eventHandler.handleException(e)) {
      log.error(e.getMessage(), e);
    }
  }

  // The body()-never-completes race hits us again... Http1xTest.testResponseBodyAfterResponseEnd()...
  @SuppressWarnings("unused")
  private volatile Future<Buffer> bodyFuture;
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static final AtomicReferenceFieldUpdater<HttpClientResponseImpl, Future<Buffer>> BODY_FUTURE_UPDATER =
    (AtomicReferenceFieldUpdater)
    AtomicReferenceFieldUpdater.newUpdater(HttpClientResponseImpl.class, Future.class, "bodyFuture");

  @Override
  public Future<Buffer> body() {
    Future<Buffer> body = bodyFuture;
    if (body != null) {
      return body;
    }
    body = onEventLoop(eventHandler::body);
    if (BODY_FUTURE_UPDATER.compareAndSet(this, null, body)) {
      request.context.dispatch(body, b -> {
        Future<Void> f = completion.future();
        if (f.isComplete()) {
          Throwable cause = f.cause();
          if (cause != null) {
            eventHandler.bodyPromise.fail(cause);
          } else {
            eventHandler.bodyPromise.complete(eventHandler.body);
          }
        }
      });
    }
    return body;
  }

  @Override
  public Future<Void> end() {
    return completion.future();
  }

  @Override
  public HttpClientResponse streamPriorityHandler(Handler<StreamPriority> handler) {
    if (handler != null) {
      checkEnded();
    }
    onEventLoop(handler, this::onEventLoopSetStreamPriorityHandler);
    return this;
  }

  private void onEventLoopSetStreamPriorityHandler(Handler<StreamPriority> handler) {
    priorityHandler = handler;
  }

  void handlePriorityChange(StreamPriority streamPriority) {
    Handler<StreamPriority> handler = priorityHandler;
    if (handler != null) {
      handler.handle(streamPriority);
    }
  }
}
