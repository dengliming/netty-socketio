/**
 * Copyright (c) 2025 The Socketio4j Project
 * Parent project : Copyright (c) 2012-2025 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.socketio4j.socketio.store.mongo;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;

import com.socketio4j.socketio.store.event.EventListener;
import com.socketio4j.socketio.store.event.EventMessage;
import com.socketio4j.socketio.store.event.EventStore;
import com.socketio4j.socketio.store.event.EventStoreMode;
import com.socketio4j.socketio.store.event.EventStoreType;
import com.socketio4j.socketio.store.event.EventType;
import com.socketio4j.socketio.store.event.PublishMode;

/**
 * MongoDB Change Streams based EventStore.
 * <p>
 * Uses MongoDB Change Streams to watch for inserts on a collection and deliver
 * events to subscribers. Each event type maps to its own collection (MULTI_CHANNEL)
 * or all events go into one collection (SINGLE_CHANNEL).
 * <p>
 * A TTL index is created on each collection to automatically expire documents
 * after a configurable retention period (default 60 seconds), preventing
 * unbounded data growth.
 * <p>
 * Requires a MongoDB replica set (standalone does not support change streams).
 */
public class MongoEventStore implements EventStore {

    private static final Logger log =
            LoggerFactory.getLogger(MongoEventStore.class);

    private static final String DEFAULT_COLLECTION_PREFIX = "socketio_events_";
    private static final long DEFAULT_TTL_SECONDS = 60;
    private static final long WATCH_STARTUP_TIMEOUT_SECONDS = 10;

    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final MongoDatabase database;
    private final Long nodeId;
    private final EventStoreMode eventStoreMode;
    private final String collectionPrefix;
    private final long ttlSeconds;

    private final ConcurrentMap<EventType, Queue<WatcherHandle>> watchers =
            new ConcurrentHashMap<>();

    private final ExecutorService watcherExecutor =
            Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "socketio-mongo-watcher");
                t.setDaemon(true);
                return t;
            });

    /**
     * Creates a new MongoEventStore.
     *
     * @param mongoClient      shared MongoDB client
     * @param databaseName     database to use for event collections
     * @param eventStoreMode   SINGLE_CHANNEL or MULTI_CHANNEL (defaults to MULTI_CHANNEL)
     * @param nodeId           node identifier used to ignore self-published events
     * @param collectionPrefix prefix for collection names (defaults to "socketio_events_")
     * @param ttlSeconds       TTL in seconds for automatic document expiry (defaults to 60)
     */
    public MongoEventStore(@NotNull MongoClient mongoClient,
                           @NotNull String databaseName,
                           @Nullable EventStoreMode eventStoreMode,
                           @Nullable Long nodeId,
                           @Nullable String collectionPrefix,
                           long ttlSeconds) {
        Objects.requireNonNull(mongoClient, "mongoClient");
        this.database = mongoClient.getDatabase(
                Objects.requireNonNull(databaseName, "databaseName"));

        this.nodeId = nodeId != null ? nodeId : getNodeId();
        this.eventStoreMode = eventStoreMode != null ? eventStoreMode : EventStoreMode.MULTI_CHANNEL;
        this.collectionPrefix = collectionPrefix != null ? collectionPrefix : DEFAULT_COLLECTION_PREFIX;
        this.ttlSeconds = ttlSeconds > 0 ? ttlSeconds : DEFAULT_TTL_SECONDS;
    }

    @Override
    public EventStoreMode getEventStoreMode() {
        return eventStoreMode;
    }

    @Override
    public EventStoreType getEventStoreType() {
        return EventStoreType.PUBSUB;
    }

    @Override
    public PublishMode getPublishMode() {
        return PublishMode.UNRELIABLE;
    }

    @Override
    public void publish0(EventType type, EventMessage msg) {
        msg.setNodeId(nodeId);

        String collectionName = getCollectionName(type);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        byte[] data;
        try {
            data = MAPPER.writeValueAsBytes(msg);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize EventMessage", e);
        }
        Document doc = new Document()
                .append("nodeId", nodeId)
                .append("eventType", type.name())
                .append("createdAt", new Date())
                .append("payload", new String(data, StandardCharsets.UTF_8));
        collection.insertOne(doc);
    }

    @Override
    public <T extends EventMessage> void subscribe0(
            EventType type,
            final EventListener<T> listener,
            Class<T> clazz) {

        String collectionName = getCollectionName(type);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        ensureTtlIndex(collection);

        WatcherHandle handle = new WatcherHandle();
        // Opening the change stream is async, but events published before the
        // cursor exists are lost. Let subscribe0 block until it is open.
        CountDownLatch opened = new CountDownLatch(1);

        watcherExecutor.submit(() -> {
            while (!handle.stopped.get()) {
                try (MongoCursor<ChangeStreamDocument<Document>> cursor =
                             collection.watch().cursor()) {
                    handle.setCursor(cursor);
                    opened.countDown();

                    while (!handle.stopped.get() && cursor.hasNext()) {
                        if (handle.stopped.get()) {
                            break;
                        }

                        ChangeStreamDocument<Document> change = cursor.next();

                        if (change.getOperationType() != OperationType.INSERT) {
                            continue;
                        }

                        if (change.getFullDocument() == null) {
                            continue;
                        }

                        try {
                            Document doc = change.getFullDocument();

                            if (EventStoreMode.MULTI_CHANNEL.equals(eventStoreMode)) {
                                String eventTypeName = doc.getString("eventType");
                                if (eventTypeName != null
                                        && !type.name().equals(eventTypeName)) {
                                    continue;
                                }
                            }

                            String payload = doc.getString("payload");
                            if (payload == null) {
                                continue;
                            }
                            T event = MAPPER.readValue(payload, clazz);
                            if (!nodeId.equals(event.getNodeId())) {
                                listener.onMessage(event);
                            }
                        } catch (Exception e) {
                            log.warn("Failed to process change event on {}", collectionName, e);
                        }
                    }
                } catch (Exception e) {
                    if (!handle.stopped.get()) {
                        log.warn("Change stream interrupted on {}, reconnecting...",
                                collectionName, e);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                } finally {
                    // Never leave subscribe0 blocked if the stream failed to open.
                    opened.countDown();
                    handle.setCursor(null);
                }
            }
        });

        watchers.computeIfAbsent(type, k -> new ConcurrentLinkedQueue<>())
                .add(handle);

        try {
            if (!opened.await(WATCH_STARTUP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Change stream on {} not established within {}s; "
                                + "early events may be missed",
                        collectionName, WATCH_STARTUP_TIMEOUT_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void unsubscribe0(EventType type) {
        Queue<WatcherHandle> handles = watchers.remove(type);
        if (handles == null || handles.isEmpty()) {
            return;
        }
        for (WatcherHandle handle : handles) {
            handle.stop();
        }
    }

    @Override
    public void shutdown0() {
        Arrays.stream(EventType.values()).forEach(this::unsubscribe);
        watchers.clear();
        watcherExecutor.shutdown();
        try {
            if (!watcherExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                watcherExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            watcherExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Creates the TTL index used to expire published events.
     * <p>
     * {@code createIndex} is idempotent for an identical index, so a failure here
     * is a real problem — an options conflict with an existing {@code createdAt}
     * index (a changed {@code ttlSeconds}) or missing privileges. Events then never
     * expire and the collection grows without bound, so log it at warn rather than
     * hiding it; subscription itself still works, so this must not abort startup.
     */
    private void ensureTtlIndex(MongoCollection<Document> collection) {
        try {
            collection.createIndex(
                    Indexes.ascending("createdAt"),
                    new IndexOptions().expireAfter(ttlSeconds, TimeUnit.SECONDS)
            );
        } catch (Exception e) {
            log.warn("Failed to create TTL index on {}; published events may never expire",
                    collection.getNamespace(), e);
        }
    }

    private String getCollectionName(EventType type) {
        if (EventStoreMode.SINGLE_CHANNEL.equals(eventStoreMode)) {
            return collectionPrefix + EventType.ALL_SINGLE_CHANNEL.name();
        }
        return collectionPrefix + type.name();
    }

    private static void closeCursorQuietly(MongoCursor<?> cursor) {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private static final class WatcherHandle {
        final AtomicBoolean stopped = new AtomicBoolean(false);
        private MongoCursor<?> cursor;

        /**
         * Publishes the cursor so {@link #stop()} can close it and unblock
         * {@code hasNext()}. If stop already happened, closes it right away —
         * otherwise the watcher would block on a cursor nobody can close.
         */
        synchronized void setCursor(MongoCursor<?> cursor) {
            if (cursor != null && stopped.get()) {
                closeCursorQuietly(cursor);
                return;
            }
            this.cursor = cursor;
        }

        synchronized void stop() {
            stopped.set(true);
            closeCursorQuietly(cursor);
            cursor = null;
        }
    }

    /**
     * Builder for {@link MongoEventStore}.
     */
    public static final class Builder {

        private final MongoClient mongoClient;
        private final String databaseName;

        private Long nodeId;
        private EventStoreMode eventStoreMode = EventStoreMode.MULTI_CHANNEL;
        private String collectionPrefix;
        private long ttlSeconds = DEFAULT_TTL_SECONDS;

        /**
         * Creates a new builder.
         *
         * @param mongoClient  shared MongoDB client (must connect to a replica set)
         * @param databaseName database to use for event collections
         */
        public Builder(@NotNull MongoClient mongoClient, @NotNull String databaseName) {
            this.mongoClient = Objects.requireNonNull(mongoClient, "mongoClient");
            this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
        }

        public Builder nodeId(long nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder eventStoreMode(@NotNull EventStoreMode mode) {
            this.eventStoreMode = Objects.requireNonNull(mode, "eventStoreMode");
            return this;
        }

        public Builder collectionPrefix(@NotNull String prefix) {
            this.collectionPrefix = Objects.requireNonNull(prefix, "collectionPrefix");
            return this;
        }

        /**
         * Sets the TTL in seconds for automatic document expiry.
         * Documents older than this are automatically removed by MongoDB.
         * Default is 60 seconds.
         *
         * @param ttlSeconds retention period in seconds
         * @return this builder
         */
        public Builder ttlSeconds(long ttlSeconds) {
            this.ttlSeconds = ttlSeconds;
            return this;
        }

        public MongoEventStore build() {
            return new MongoEventStore(
                    mongoClient,
                    databaseName,
                    eventStoreMode,
                    nodeId,
                    collectionPrefix,
                    ttlSeconds
            );
        }
    }
}
