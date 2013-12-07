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

import static com.google.common.base.Throwables.propagate;
import static com.hp.hpl.jena.rdf.model.ResourceFactory.createProperty;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static org.fcrepo.kernel.RdfLexicon.REPOSITORY_NAMESPACE;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import org.apache.http.HttpException;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.fcrepo.kernel.utils.EventType;
import org.slf4j.Logger;

import com.hp.hpl.jena.rdf.model.Property;


/**
 * MessageListener implementation that retrieves objects from the repository and
 * invokes one or more indexers to index the content.
 *
 * documentation:
 * https://wiki.duraspace.org/display/FF/Design+-+Messaging+for+Workflow
 * @author Esmé Cowles
 * @author ajs6f
 * @date Aug 19 2013
 **/
public class IndexerGroup implements MessageListener {

    private static final Logger LOGGER = getLogger(IndexerGroup.class);

    private String repositoryURL;

    private Set<Indexer> indexers;

    private final HttpClient httpClient;


    /**
     * Identifier message header
     */
    private static final String IDENTIFIER_HEADER_NAME = REPOSITORY_NAMESPACE
            + "identifier";

    /**
     * Event type message header
     */
    private static final String EVENT_TYPE_HEADER_NAME = REPOSITORY_NAMESPACE
            + "eventType";

    /**
     * Type of event that qualifies as a removal.
     */
    private static final String REMOVAL_EVENT_TYPE = REPOSITORY_NAMESPACE
            + EventType.valueOf(NODE_REMOVED).toString();

    /**
     * Indicates the transformation to use with this resource to derive indexing information.
     */
    public static final Property INDEXING_TRANSFORM_PREDICATE =
        createProperty(REPOSITORY_NAMESPACE + "hasIndexingTransformation");

    private static final Reader EMPTY_CONTENT = null;

    /**
     * Default constructor.
     **/
    public IndexerGroup() {
        LOGGER.debug("Creating IndexerGroup: {}", this);
        this.httpClient = new DefaultHttpClient();
    }

    /**
     * Set repository URL.
     **/
    public void setRepositoryURL(final String repositoryURL) {
        this.repositoryURL = repositoryURL;
    }

    /**
     * Get repository URL.
     **/
    public String getRepositoryURL() {
        return repositoryURL;
    }

    /**
     * Set indexers for this group.
     **/
    public void setIndexers(final Set<Indexer> indexers) {
        this.indexers = indexers;
        LOGGER.debug("Using indexer complement: {} ", indexers);
    }

    /**
     * Get indexers set for this group.
     **/
    public Set<Indexer> getIndexers() {
        return indexers;
    }

    /**
     * Handle a JMS message representing an object update or deletion event.
     **/
    @Override
    public void onMessage(final Message message) {
        try {
            LOGGER.debug("Received message: {}", message.getJMSMessageID());
        } catch (final JMSException e) {
            LOGGER.error("Received unintelligible message: {}", e);
            propagate(e);
        }
        try {
            // get pid and eventType from message
            final String pid = message.getStringProperty(IDENTIFIER_HEADER_NAME);
            final String eventType = message.getStringProperty(EVENT_TYPE_HEADER_NAME);

            LOGGER.debug("Discovered pid: {} in message.", pid);
            LOGGER.debug("Discovered event type: {} in message.", eventType);

            final Boolean removal = REMOVAL_EVENT_TYPE.equals(eventType);
            LOGGER.debug("It is {} that this is a removal operation.", removal);

            final NamedFieldsRetriever nfr =
                new NamedFieldsRetriever(getRepositoryURL() + pid, httpClient);
            final RdfRetriever rdfr =
                new RdfRetriever(getRepositoryURL() + pid, httpClient);

            for (final Indexer indexer : indexers) {
                LOGGER.debug("Operating for indexer: {}", indexer);
                Boolean hasContent = false;
                Reader content = EMPTY_CONTENT;
                if (!removal) {
                    switch (indexer.getIndexerType()) {
                        case NAMEDFIELDS:
                            try (final InputStream result = nfr.call()) {
                                content = new InputStreamReader(result);
                                hasContent = true;
                            } catch (final IOException | HttpException e) {
                                LOGGER.error(
                                        "Could not retrieve content for update!",
                                        e);
                                hasContent = false;
                            } catch (final CannotTransformToNamedFieldsException e) {
                                hasContent = false;
                            }
                        case RDF:
                            try (final InputStream result = rdfr.call()) {
                                content = new InputStreamReader(result);
                                hasContent = true;
                            } catch (IOException | HttpException e) {
                                LOGGER.error(
                                        "Could not retrieve content for update!",
                                        e);
                                hasContent = false;
                            } catch (final CannotTransformToNamedFieldsException e1) {
                                hasContent = false;
                            }
                        default:
                            content = new StringReader(pid);
                            hasContent = true;
                    }
                }

                try {
                    if (removal) {
                        indexer.remove(pid);
                    } else {
                        if (hasContent) {
                            indexer.update(pid, content);
                        } else {
                            LOGGER.error(
                                "Received update for {} but was unable to retrieve content for update!",
                                pid);
                        }
                    }
                } catch (final Exception e) {
                    LOGGER.error("Error indexing {}: {}!", pid, e);
                }
            }
        } catch (final JMSException e) {
            LOGGER.error("Error processing JMS event!", e);
        }
    }

}
