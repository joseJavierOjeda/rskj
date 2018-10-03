/*
 * This file is part of RskJ
 * Copyright (C) 2017 RSK Labs Ltd.
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

package co.rsk.mine;

import org.ethereum.rpc.TypeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nonnull;
import java.math.BigInteger;

/**
 * MinerClient for automine setting
 * @author Ignacio Pulice
 */
public class MinerClientInstantImpl implements MinerClient {
    private long nextNonceToUse = 0;
    private static final Logger logger = LoggerFactory.getLogger("minerClient");
    private final MinerServer minerServer;
    private volatile boolean stop = false;
    private volatile boolean isMining = false;
    private volatile MinerWork work;

    public MinerClientInstantImpl(MinerServer minerServer) {
        this.minerServer = minerServer;
    }

    public void mine() {
        isMining = true;
    }

    public boolean isMining() {
        return this.isMining;
    }

    @Override
    public boolean mineBlock() {
        //if miner server was stopped for some reason, we don't mine.
        if(stop){
            return false;
        }

        work = minerServer.getWork();

        co.rsk.bitcoinj.core.NetworkParameters bitcoinNetworkParameters = co.rsk.bitcoinj.params.RegTestParams.get();
        co.rsk.bitcoinj.core.BtcTransaction bitcoinMergedMiningCoinbaseTransaction = MinerUtils.getBitcoinMergedMiningCoinbaseTransaction(bitcoinNetworkParameters, work);
        co.rsk.bitcoinj.core.BtcBlock bitcoinMergedMiningBlock = MinerUtils.getBitcoinMergedMiningBlock(bitcoinNetworkParameters, bitcoinMergedMiningCoinbaseTransaction);

        BigInteger target = new BigInteger(1, TypeConverter.stringHexToByteArray(work.getTarget()));
        boolean foundNonce = findNonce(bitcoinMergedMiningBlock, target);

        if (foundNonce) {
            logger.info("Mined block: " + work.getBlockHashForMergedMining());
            minerServer.submitBitcoinBlock(work.getBlockHashForMergedMining(), bitcoinMergedMiningBlock);
        }

        return foundNonce;
    }

    @Override
    public boolean fallbackMineBlock() {
        //TODO: what should we do here ?
        return false;
    }

    /**
     * findNonce will try to find a valid nonce for bitcoinMergedMiningBlock, that satisfies the given target difficulty.
     *
     * @param bitcoinMergedMiningBlock bitcoinBlock to find nonce for. This block's nonce will be modified.
     * @param target                   target difficulty. Block's hash should be lower than this number.
     * @return true if a nonce was found, false otherwise.
     * @remarks This method will return if the stop or newBetBlockArrivedFromAnotherNode intance variables are set to true.
     */

    private boolean findNonce(@Nonnull final co.rsk.bitcoinj.core.BtcBlock bitcoinMergedMiningBlock,
                              @Nonnull final BigInteger target) {
        bitcoinMergedMiningBlock.setNonce(nextNonceToUse++);

        while (true) {
            // Is our proof of work valid yet?
            BigInteger blockHashBI = bitcoinMergedMiningBlock.getHash().toBigInteger();
            if (blockHashBI.compareTo(target) <= 0) {
                return true;
            }
            // No, so increment the nonce and try again.
            bitcoinMergedMiningBlock.setNonce(nextNonceToUse++);
            if (bitcoinMergedMiningBlock.getNonce() % 100000 == 0) {
                logger.debug("Solving block. Nonce: " + bitcoinMergedMiningBlock.getNonce());
            }
        }
    }

    public void stop() {
        stop = true;
        isMining = false;
    }
}
