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

import static com.hp.hpl.jena.rdf.model.ModelFactory.createDefaultModel;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createResource;
import static org.fcrepo.indexer.IndexerGroup.INDEXING_TRANSFORM_PREDICATE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.jena.riot.RiotNotFoundException;
import org.slf4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;

/**
 * Retrieves resources transformed into sets of named fields via LDPath.
 * For use with indexers like Solr.
 *
 * @author ajs6f
 * @date Dec 6, 2013
 */
public class NamedFieldsRetriever extends CachingRetriever {

    private final String uri;

    private final HttpClient httpClient;

    private static final Logger LOGGER = getLogger(NamedFieldsRetriever.class);

    /**
     * @param uri
     * @param client
     */
    public NamedFieldsRetriever(final String uri, final HttpClient client) {
        this.uri = uri;
        this.httpClient = client;
    }

    @Override
    public HttpResponse retrieveHttpResponse() throws CannotTransformToNamedFieldsException,
        ClientProtocolException, IOException {
        LOGGER.debug("Retrieving RDF representation from: {}", uri);
        String transformKey;
        try {
            final Model rdf = createDefaultModel().read(uri);
            if (!rdf.contains(createResource(uri), INDEXING_TRANSFORM_PREDICATE)) {
                throw new CannotTransformToNamedFieldsException(uri);
            }
            final RDFNode indexingTransform =
                rdf.listObjectsOfProperty(createResource(uri),
                        INDEXING_TRANSFORM_PREDICATE).next();
            transformKey = indexingTransform.asLiteral().getString();
        } catch (final RiotNotFoundException e) {
            LOGGER.error("Couldn't retrieve representation of resource to determine its indexability!");
            throw new CannotTransformToNamedFieldsException(uri);
        }

        LOGGER.debug("Discovered transform key: {}", transformKey);
        final HttpGet transformedResourceRequest =
            new HttpGet(uri + "/fcr:transform/" + transformKey);
        LOGGER.debug("Retrieving transformed resource from: {}",
                transformedResourceRequest.getURI());
        return
            httpClient.execute(transformedResourceRequest);
    }

}
