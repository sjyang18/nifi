/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.services.azure.storage;

import com.microsoft.azure.storage.StorageCredentials;

public class AzureStorageCredentialsDetails {

    private final String storageAccountName;

    private final StorageCredentials storageCredentials;

    private final String endPointSuffix;

    public AzureStorageCredentialsDetails(String storageAccountName, StorageCredentials storageCredentials){
        this.storageAccountName = storageAccountName;
        this.storageCredentials = storageCredentials;
        this.endPointSuffix = null;
    }

    public AzureStorageCredentialsDetails(String storageAccountName, StorageCredentials storageCredentials, String endPointSuffix) {
        this.storageAccountName = storageAccountName;
        this.storageCredentials = storageCredentials;
        this.endPointSuffix = endPointSuffix;
    }

    public String getStorageAccountName() {
        return storageAccountName;
    }

    public StorageCredentials getStorageCredentials() {
        return storageCredentials;
    }

    public String getEndpointSuffix() {
        if (this.endPointSuffix == null || this.endPointSuffix.length() ==0) {
            return null; // azure sdk expects to get null for default (core.windows.net)
        }
        else return endPointSuffix;
    }
}
