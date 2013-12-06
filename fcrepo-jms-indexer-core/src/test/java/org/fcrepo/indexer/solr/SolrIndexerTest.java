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
package org.fcrepo.indexer.solr;

import static org.apache.solr.core.CoreContainer.createAndLoad;
import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.fcrepo.indexer.solr.SolrIndexer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;


/**
 * @author yecao
 * @author ajs6f
 * @date Nov 2013
 */
public class SolrIndexerTest {

    private final String solrHome = "target/test-classes/solr";

    private SolrIndexer indexer;

    private SolrServer server;

    private static final Logger LOGGER = getLogger(SolrIndexerTest.class);

    @Before
    public void setUp() throws Exception {
        final CoreContainer container =
            createAndLoad(solrHome, new File(solrHome, "solr.xml"));
        LOGGER.debug("Using Solr home: {}", new File(container.getSolrHome())
                .getAbsolutePath());
        server = new EmbeddedSolrServer(container, "testCore");
        indexer = new SolrIndexer(server);
    }

    /**
     * Test method for
     * {@link org.fcrepo.indexer.solr.SolrIndexer#update(java.lang.String, java.lang.String)}
     * .
     *
     * @throws SolrServerException
     */
    @Test
    public void testUpdate() throws SolrServerException {
        doUpdate("456");
    }

    private void doUpdate(final String pid) throws SolrServerException {
        final String content = "{\"id\" : \"" +pid+ "\"}";
        LOGGER.debug(
                "Trying update operation with identifier: {} and content: \"{}\".",
                pid, content);
        indexer.update(pid, new StringReader(content));
        final SolrQuery numRows = new SolrQuery("*:*").setRows(0);
        LOGGER.debug("Index now contains: {} records.", server.query(numRows)
                .getResults().getNumFound());
        final SolrParams params = new SolrQuery("id:" + pid);
        final QueryResponse response = server.query(params);
        assertEquals("Didn't find our expected record!", 1L, response
                .getResults().getNumFound());
    }

    /**
     * Test method for
     * {@link org.fcrepo.indexer.solr.SolrIndexer#remove(java.lang.String)}.
     *
     * @throws IOException
     * @throws SolrServerException
     */
    @Test
    public void testRemove() throws IOException, SolrServerException {
        final String pid = "123";
        doUpdate(pid);
        indexer.remove(pid);
        final SolrParams params = new SolrQuery("id:" + pid);
        final QueryResponse response = server.query(params);
        assertEquals(0, response.getResults().getNumFound());
    }

}
