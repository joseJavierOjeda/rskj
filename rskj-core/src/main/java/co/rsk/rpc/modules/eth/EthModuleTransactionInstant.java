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

package co.rsk.rpc.modules.eth;

import co.rsk.config.RskSystemProperties;
import co.rsk.core.RskAddress;
import co.rsk.core.Wallet;
import co.rsk.mine.MinerClient;
import co.rsk.mine.MinerServer;
import com.sun.xml.internal.bind.v2.TODO;
import org.bouncycastle.util.encoders.Hex;
import org.ethereum.core.*;
import org.ethereum.facade.Ethereum;
import org.ethereum.rpc.TypeConverter;
import org.ethereum.rpc.Web3;
import org.ethereum.vm.GasCost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigInteger;
import static org.ethereum.rpc.TypeConverter.stringHexToByteArray;

public class EthModuleTransactionInstant implements EthModuleTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger("web3");

    private final RskSystemProperties config;
    private final Ethereum eth;
    private final Wallet wallet;
    private final TransactionPool transactionPool;
    private final MinerServer minerServer;
    private final MinerClient minerClient;
    private final Blockchain blockchain;

    public EthModuleTransactionInstant(RskSystemProperties config, Ethereum eth, Wallet wallet, TransactionPool transactionPool, MinerServer minerServer, MinerClient minerClient, Blockchain blockchain) {
        this.config = config;
        this.eth = eth;
        this.wallet = wallet;
        this.transactionPool = transactionPool;
        this.minerServer = minerServer;
        this.minerClient = minerClient;
        this.blockchain = blockchain;
    }

    @Override
    public synchronized String sendTransaction(Web3.CallArguments args) {
        Account account = this.wallet.getAccount(new RskAddress(args.from));
        String s = null;
        try {
            String toAddress = args.to != null ? Hex.toHexString(stringHexToByteArray(args.to)) : null;

            BigInteger value = args.value != null ? TypeConverter.stringNumberAsBigInt(args.value) : BigInteger.ZERO;
            BigInteger gasPrice = args.gasPrice != null ? TypeConverter.stringNumberAsBigInt(args.gasPrice) : BigInteger.ZERO;
            BigInteger gasLimit = args.gas != null ? TypeConverter.stringNumberAsBigInt(args.gas) : BigInteger.valueOf(GasCost.TRANSACTION_DEFAULT);

            if (args.data != null && args.data.startsWith("0x")) {
                args.data = args.data.substring(2);
            }

            synchronized (transactionPool) {
                BigInteger accountNonce = args.nonce != null ? TypeConverter.stringNumberAsBigInt(args.nonce) : transactionPool.getPendingState().getNonce(account.getAddress());
                Transaction tx = Transaction.create(config, toAddress, value, accountNonce, gasPrice, gasLimit, args.data);
                tx.sign(account.getEcKey().getPrivKeyBytes());
                eth.submitTransaction(tx.toImmutableTransaction());
                s = tx.getHash().toJsonString();
            }

            Block newBlock = minerServer.getBlockToBeMine(blockchain.getBestBlock());

            //Check if the transaction which was send could get in the block
            Transaction txInBlock = null;
            for (Transaction x : newBlock.getTransactionsList()) {
                if (x.getHash().toJsonString().equals(s)) {
                    txInBlock = x;
                    break;
                }
            }

            //AutoMine setting should mine the block if the transaction is in the block
            //TODO: what should we do if transaction is not in the block for some reason ?
            if(txInBlock != null){
                minerServer.buildBlockToMine(blockchain.getBestBlock(), false);
                minerClient.mineBlock();
            }

            return s;
        } finally {
            LOGGER.debug("eth_sendTransaction({}): {}", args, s);
        }
    }
}