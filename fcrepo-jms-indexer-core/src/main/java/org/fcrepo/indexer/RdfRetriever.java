/**
 * Copyright 2013 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fcrepo.indexer;

import static org.apache.http.HttpStatus.SC_OK;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;

/**
 * Retrieves RDF representations of resources for storage in a triplestore.
 * TODO: Extend functionality to provide for transformation, a la
 * {@link NamedFieldsRetriever}
 *
 * @author ajs6f
 * @date Dec 6, 2013
 */
public class RdfRetriever implements IndexableContentRetriever {

    private static final String RDF_SERIALIZATION = "application/rdf+xml";

    private final String identifier;

    private final HttpClient httpClient;

    private Boolean cached = false;

    private byte[] cache;

    /**
     * @param identifier
     * @param client
     */
    public RdfRetriever(final String identifier, final HttpClient client) {
        this.identifier = identifier;
        this.httpClient = client;
    }

    @Override
    public InputStream call() throws ClientProtocolException, IOException,
        HttpException {
        if (cached) {
            return new ByteArrayInputStream(cache);
        }
        final HttpUriRequest request = new HttpGet(identifier);
        request.addHeader("Accept", RDF_SERIALIZATION);
        final HttpResponse response = httpClient.execute(request);
        if (response.getStatusLine().getStatusCode() == SC_OK) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                response.getEntity().writeTo(out);
                cache = out.toByteArray();
            }
            cached = true;
            return new ByteArrayInputStream(cache);
        } else {
            throw new HttpException(response.getStatusLine().getReasonPhrase());
        }
    }

}
