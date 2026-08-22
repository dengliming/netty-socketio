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

import java.util.Arrays;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
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
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

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
 * Requires a MongoDB replica set (standalone does not support change streams).
 */
public class MongoEventStore implements EventStore {

    private static final Logger log =
            LoggerFactory.getLogger(MongoEventStore.class);

    private static final String DEFAULT_COLLECTION_PREFIX = "socketio_events_";

    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private final MongoClient mongoClient;
    private final MongoDatabase database;
    private final Long nodeId;
    private final EventStoreMode eventStoreMode;
    private final String collectionPrefix;

    private final ConcurrentMap<EventType, Queue<WatcherHandle>> watchers =
            new ConcurrentHashMap<>();

    private final ExecutorService watcherExecutor =
            Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r, "socketio-mongo-watcher");
                t.setDaemon(true);
                return t;
            });

    public MongoEventStore(@NotNull MongoClient mongoClient,
                           @NotNull String databaseName,
                           @Nullable EventStoreMode eventStoreMode,
                           @Nullable Long nodeId,
                           @Nullable String collectionPrefix) {
        this.mongoClient = Objects.requireNonNull(mongoClient, "mongoClient");
        this.database = mongoClient.getDatabase(
                Objects.requireNonNull(databaseName, "databaseName"));

        if (nodeId == null) {
            nodeId = getNodeId();
        }
        this.nodeId = nodeId;

        if (eventStoreMode == null) {
            eventStoreMode = EventStoreMode.MULTI_CHANNEL;
        }
        this.eventStoreMode = eventStoreMode;

        this.collectionPrefix = collectionPrefix != null
                ? collectionPrefix : DEFAULT_COLLECTION_PREFIX;
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

        try {
            String collectionName = getCollectionName(type);
            MongoCollection<Document> collection = database.getCollection(collectionName);
            byte[] data = MAPPER.writeValueAsBytes(msg);
            Document doc = new Document()
                    .append("nodeId", nodeId)
                    .append("eventType", type.name())
                    .append("payload", new String(data, "UTF-8"));
            collection.insertOne(doc);
        } catch (Exception e) {
            log.warn("Failed to publish event {}", type, e);
        }
    }

    @Override
    public <T extends EventMessage> void subscribe0(
            EventType type,
            final EventListener<T> listener,
            Class<T> clazz) {

        String collectionName = getCollectionName(type);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        WatcherHandle handle = new WatcherHandle();

        watcherExecutor.submit(() -> {
            while (!handle.stopped.get()) {
                try {
                    ChangeStreamIterable<Document> changeStream = collection.watch()
                            .fullDocument(FullDocument.UPDATE_LOOKUP);

                    for (ChangeStreamDocument<Document> change : changeStream) {
                        if (handle.stopped.get()) {
                            break;
                        }
                        if (change.getFullDocument() == null) {
                            continue;
                        }
                        try {
                            Document doc = change.getFullDocument();
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
                }
            }
        });

        watchers.computeIfAbsent(type, k -> new ConcurrentLinkedQueue<>())
                .add(handle);
    }

    @Override
    public void unsubscribe0(EventType type) {
        Queue<WatcherHandle> handles = watchers.remove(type);
        if (handles == null || handles.isEmpty()) {
            return;
        }
        for (WatcherHandle handle : handles) {
            handle.stopped.set(true);
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

    private String getCollectionName(EventType type) {
        if (EventStoreMode.SINGLE_CHANNEL.equals(eventStoreMode)) {
            return collectionPrefix + EventType.ALL_SINGLE_CHANNEL.name();
        }
        return collectionPrefix + type.name();
    }

    private static final class WatcherHandle {
        final AtomicBoolean stopped = new AtomicBoolean(false);
    }

    public static final class Builder {

        private final MongoClient mongoClient;
        private final String databaseName;

        private Long nodeId;
        private EventStoreMode eventStoreMode = EventStoreMode.MULTI_CHANNEL;
        private String collectionPrefix;

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

        public MongoEventStore build() {
            return new MongoEventStore(
                    mongoClient,
                    databaseName,
                    eventStoreMode,
                    nodeId,
                    collectionPrefix
            );
        }
    }
}
