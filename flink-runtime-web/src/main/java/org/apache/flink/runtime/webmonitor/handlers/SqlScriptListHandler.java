/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static java.util.Objects.requireNonNull;

/** Handle request for listing uploaded sql scripts. */
public class SqlScriptListHandler
        extends AbstractRestHandler<
        RestfulGateway, EmptyRequestBody, JarListInfo, EmptyMessageParameters> {

    private static final File[] EMPTY_FILES_ARRAY = new File[0];

    private final CompletableFuture<String> localAddressFuture;

    private final File sqlScriptDir;

    private final Configuration configuration;

    private final Executor executor;

    public SqlScriptListHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, JarListInfo, EmptyMessageParameters> messageHeaders,
            CompletableFuture<String> localAddressFuture,
            File sqlScriptDir,
            Configuration configuration,
            Executor executor) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);

        this.localAddressFuture = localAddressFuture;
        this.sqlScriptDir = requireNonNull(sqlScriptDir);
        this.configuration = configuration;
        this.executor = requireNonNull(executor);
    }

    @Override
    protected CompletableFuture<JarListInfo> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        final String localAddress;
        Preconditions.checkState(localAddressFuture.isDone());

        try {
            localAddress = localAddressFuture.get();
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }

        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        final File[] list = getSqlScriptFiles();
                        final List<JarListInfo.JarFileInfo> jarFileList =
                                new ArrayList<>(list.length);
                        for (File f : list) {
                            // separate the uuid and the name parts.
                            String name = f.getName();
                            if (!name.endsWith(".sql")) {
                                continue;
                            }

                            List<JarListInfo.JarEntryInfo> jarEntryList = new ArrayList<>();
                            String[] classes = new String[0];
                            try (JarFile jar = new JarFile(f)) {
                                Manifest manifest = jar.getManifest();
                                String assemblerClass = null;

                                if (manifest != null) {
                                    assemblerClass =
                                            manifest.getMainAttributes()
                                                    .getValue(
                                                            PackagedProgram
                                                                    .MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS);
                                    if (assemblerClass == null) {
                                        assemblerClass =
                                                manifest.getMainAttributes()
                                                        .getValue(
                                                                PackagedProgram
                                                                        .MANIFEST_ATTRIBUTE_MAIN_CLASS);
                                    }
                                }
                                if (assemblerClass != null) {
                                    classes = assemblerClass.split(",");
                                }
                            } catch (IOException ignored) {
                                // we simply show no entries here
                            }

                            // show every entry class that can be loaded later on.
                            for (String clazz : classes) {
                                clazz = clazz.trim();

                                try (PackagedProgram program =
                                             PackagedProgram.newBuilder()
                                                     .setJarFile(f)
                                                     .setEntryPointClassName(clazz)
                                                     .setConfiguration(configuration)
                                                     .build()) {
                                    JarListInfo.JarEntryInfo jarEntryInfo =
                                            new JarListInfo.JarEntryInfo(
                                                    clazz, program.getDescription());
                                    jarEntryList.add(jarEntryInfo);
                                } catch (Exception ignored) {
                                    // ignore jar files which throw an error upon creating a
                                    // PackagedProgram
                                }
                            }

                            jarFileList.add(
                                    new JarListInfo.JarFileInfo(
                                            name, name, f.lastModified(), jarEntryList));
                        }

                        return new JarListInfo(localAddress, jarFileList);
                    } catch (Exception e) {
                        throw new CompletionException(
                                new FlinkException("Failed to fetch jar list.", e));
                    }
                },
                executor);
    }

    private File[] getSqlScriptFiles() {
        if (sqlScriptDir == null) {
            return EMPTY_FILES_ARRAY;
        }

        final File[] list = sqlScriptDir.listFiles((dir, name) -> name.endsWith(".sql"));
        if (list == null) {
            log.warn(
                    "Sql script upload dir {} does not exist, or had been deleted externally. "
                            + "Previously uploaded jars are no longer available.",
                    sqlScriptDir);
            return EMPTY_FILES_ARRAY;
        } else {
            // last modified ascending order
            Arrays.sort(list, (f1, f2) -> Long.compare(f2.lastModified(), f1.lastModified()));
            return list;
        }
    }
}
