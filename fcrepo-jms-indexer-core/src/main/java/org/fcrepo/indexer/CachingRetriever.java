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
/**
 *
 */
package org.fcrepo.indexer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;


/**
 * @author ajs6f
 * @date Dec 7, 2013
 */
public abstract class CachingRetriever implements IndexableContentRetriever {

    private Boolean cached = false;

    private byte[] cache;

    /* (non-Javadoc)
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public InputStream call() throws ClientProtocolException, IOException,
        CannotTransformToNamedFieldsException, HttpException {
        if (cached) {
            return new ByteArrayInputStream(cache);
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            retrieveHttpResponse().getEntity().writeTo(out);
            cache = out.toByteArray();
        }
        cached = true;
        return new ByteArrayInputStream(cache);
    }

    protected abstract HttpResponse retrieveHttpResponse()
        throws CannotTransformToNamedFieldsException,
        ClientProtocolException, IOException, HttpException;

}
