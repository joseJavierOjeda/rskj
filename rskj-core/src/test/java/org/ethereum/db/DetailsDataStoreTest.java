/*
 * This file is part of RskJ
 * Copyright (C) 2017 RSK Labs Ltd.
 * (derived from ethereumJ library, Copyright (c) 2016 <ether.camp>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.ethereum.db;

import co.rsk.config.TestSystemProperties;
import co.rsk.core.RskAddress;
import co.rsk.db.ContractDetailsImpl;
import co.rsk.trie.TrieImpl;
import co.rsk.trie.TrieStore;
import co.rsk.trie.TrieStoreImpl;
import org.ethereum.datasource.HashMapDB;
import org.ethereum.vm.DataWord;
import org.junit.Test;
import org.bouncycastle.util.encoders.Hex;

import static org.ethereum.TestUtils.randomAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DetailsDataStoreTest {

    private final TestSystemProperties config = new TestSystemProperties();

    @Test
    public void test1(){
        HashMapDB keyValueDataSource = new HashMapDB();
        DatabaseImpl db = new DatabaseImpl(new HashMapDB());
        TrieStore.Factory trieStoreFactory = name -> new TrieStoreImpl(keyValueDataSource);
        DetailsDataStore dds = new DetailsDataStore(db, trieStoreFactory, config.detailsInMemoryStorageLimit());

        RskAddress c_key = new RskAddress("0000000000000000000000000000000000001a2b");
        byte[] code = Hex.decode("60606060");
        byte[] key =  Hex.decode("11");
        byte[] value =  Hex.decode("aa");

        ContractDetails contractDetails = new ContractDetailsImpl(
            null,
            new TrieImpl(new TrieStoreImpl(keyValueDataSource), true),
            null,
            trieStoreFactory,
            config.detailsInMemoryStorageLimit()
        );
        contractDetails.setAddress(randomAddress().getBytes());
        contractDetails.setCode(code);
        contractDetails.put(new DataWord(key), new DataWord(value));

        dds.update(c_key, contractDetails);

        ContractDetails contractDetails_ = dds.get(c_key);

        String encoded1 = Hex.toHexString(contractDetails.getEncoded());
        String encoded2 = Hex.toHexString(contractDetails_.getEncoded());

        assertEquals(encoded1, encoded2);

        dds.flush();

        contractDetails_ = dds.get(c_key);
        encoded2 = Hex.toHexString(contractDetails_.getEncoded());
        assertEquals(encoded1, encoded2);
    }

    @Test
    public void test2(){

        DatabaseImpl db = new DatabaseImpl(new HashMapDB());
        TrieStore.Factory trieStoreFactory = name -> new TrieStoreImpl(new HashMapDB());
        DetailsDataStore dds = new DetailsDataStore(db, trieStoreFactory, config.detailsInMemoryStorageLimit());

        RskAddress c_key = new RskAddress("0000000000000000000000000000000000001a2b");
        byte[] code = Hex.decode("60606060");
        byte[] key =  Hex.decode("11");
        byte[] value =  Hex.decode("aa");

        ContractDetails contractDetails = new ContractDetailsImpl(
            null,
            new TrieImpl(new TrieStoreImpl(new HashMapDB()), true),
            null,
            trieStoreFactory,
            config.detailsInMemoryStorageLimit()
        );
        contractDetails.setCode(code);
        contractDetails.setAddress(randomAddress().getBytes());
        contractDetails.put(new DataWord(key), new DataWord(value));

        dds.update(c_key, contractDetails);

        ContractDetails contractDetails_ = dds.get(c_key);

        String encoded1 = Hex.toHexString(contractDetails.getEncoded());
        String encoded2 = Hex.toHexString(contractDetails_.getEncoded());

        assertEquals(encoded1, encoded2);

        dds.remove(c_key);

        contractDetails_ = dds.get(c_key);
        assertNull(contractDetails_);

        dds.flush();

        contractDetails_ = dds.get(c_key);
        assertNull(contractDetails_);
    }

    @Test
    public void test3(){

        HashMapDB store = new HashMapDB();
        DatabaseImpl db = new DatabaseImpl(new HashMapDB());
        TrieStore.Factory trieStoreFactory = name -> new TrieStoreImpl(store);
        DetailsDataStore dds = new DetailsDataStore(db, trieStoreFactory, config.detailsInMemoryStorageLimit());

        RskAddress c_key = new RskAddress("0000000000000000000000000000000000001a2b");
        byte[] code = Hex.decode("60606060");
        byte[] key =  Hex.decode("11");
        byte[] value =  Hex.decode("aa");

        ContractDetails contractDetails = new ContractDetailsImpl(
            null,
            new TrieImpl(new TrieStoreImpl(store), true),
            null,
            trieStoreFactory,
            config.detailsInMemoryStorageLimit()
        );
        contractDetails.setCode(code);
        contractDetails.put(new DataWord(key), new DataWord(value));

        dds.update(c_key, contractDetails);

        ContractDetails contractDetails_ = dds.get(c_key);

        String encoded1 = Hex.toHexString(contractDetails.getEncoded());
        String encoded2 = Hex.toHexString(contractDetails_.getEncoded());

        assertEquals(encoded1, encoded2);

        dds.remove(c_key);
        dds.update(c_key, contractDetails);

        contractDetails_ = dds.get(c_key);
        encoded2 = Hex.toHexString(contractDetails_.getEncoded());
        assertEquals(encoded1, encoded2);

        dds.flush();

        contractDetails_ = dds.get(c_key);
        encoded2 = Hex.toHexString(contractDetails_.getEncoded());
        assertEquals(encoded1, encoded2);
    }

    @Test
    public void test4() {

        DatabaseImpl db = new DatabaseImpl(new HashMapDB());
        TrieStore.Factory trieStoreFactory = name -> new TrieStoreImpl(new HashMapDB());
        DetailsDataStore dds = new DetailsDataStore(db, trieStoreFactory, config.detailsInMemoryStorageLimit());

        RskAddress c_key = new RskAddress("0000000000000000000000000000000000001a2b");

        ContractDetails contractDetails = dds.get(c_key);
        assertNull(contractDetails);
    }
}
