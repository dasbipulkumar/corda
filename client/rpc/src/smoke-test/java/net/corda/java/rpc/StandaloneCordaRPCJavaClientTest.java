package net.corda.java.rpc;

import net.corda.client.rpc.CordaRPCConnection;
import net.corda.core.contracts.Amount;
import net.corda.core.messaging.CordaRPCOps;
import net.corda.core.messaging.DataFeed;
import net.corda.core.messaging.FlowHandle;
import net.corda.core.node.NodeInfo;
import net.corda.core.node.services.NetworkMapCache;
import net.corda.core.transactions.SignedTransaction;
import net.corda.core.utilities.OpaqueBytes;
import net.corda.flows.CashIssueFlow;
import net.corda.nodeapi.User;
import net.corda.smoketesting.NodeConfig;
import net.corda.smoketesting.NodeProcess;
import org.bouncycastle.asn1.x500.X500Name;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.singletonList;
import static kotlin.test.AssertionsKt.assertEquals;
import static net.corda.contracts.GetBalances.getCashBalance;

public class StandaloneCordaRPCJavaClientTest {
    private List<String> perms = singletonList("ALL");
    private Set<String> permSet = new HashSet<>(perms);
    private User rpcUser = new User("user1", "test", permSet);

    private AtomicInteger port = new AtomicInteger(15000);

    private NodeProcess notary;
    private CordaRPCOps rpcProxy;
    private CordaRPCConnection connection;
    private NodeInfo notaryNode;

    private NodeConfig notaryConfig = new NodeConfig(
            new X500Name("CN=Notary Service,O=R3,OU=corda,L=Zurich,C=CH"),
            port.getAndIncrement(),
            port.getAndIncrement(),
            port.getAndIncrement(),
            singletonList("corda.notary.validating"),
            singletonList(rpcUser),
            null
    );

    @Before
    public void setUp() {
        notary = new NodeProcess.Factory().create(notaryConfig);
        connection = notary.connect();
        rpcProxy = connection.getProxy();
        notaryNode = fetchNotaryIdentity();
    }

    @After
    public void done() {
        try {
            connection.close();
        } finally {
            notary.close();
        }
    }

    private NodeInfo fetchNotaryIdentity() {
        DataFeed<List<NodeInfo>, NetworkMapCache.MapChange> nodeDataFeed = rpcProxy.networkMapFeed();
        return nodeDataFeed.getSnapshot().get(0);
    }

    @Test
    public void testCashBalances() throws NoSuchFieldException, ExecutionException, InterruptedException {
        Amount<Currency> dollars123 = new Amount<>(123, Currency.getInstance("USD"));

        FlowHandle<SignedTransaction> flowHandle = rpcProxy.startFlowDynamic(CashIssueFlow.class,
                dollars123, OpaqueBytes.of("1".getBytes()),
                notaryNode.getLegalIdentity(), notaryNode.getLegalIdentity());
        System.out.println("Started issuing cash, waiting on result");
        flowHandle.getReturnValue().get();

        Amount<Currency> balance = getCashBalance(rpcProxy, Currency.getInstance("USD"));
        System.out.print("Balance: " + balance + "\n");

        assertEquals(dollars123, balance, "matching");
    }
}
