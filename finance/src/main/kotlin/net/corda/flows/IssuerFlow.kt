package net.corda.flows

import co.paralleluniverse.fibers.Suspendable
import net.corda.contracts.asset.Cash
import net.corda.core.contracts.*
import net.corda.core.flows.*
import net.corda.core.identity.AbstractParty
import net.corda.core.identity.AnonymousParty
import net.corda.core.identity.Party
import net.corda.core.serialization.CordaSerializable
import net.corda.core.transactions.SignedTransaction
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.ProgressTracker
import net.corda.core.utilities.unwrap
import java.util.*

/**
 *  This flow enables a client to request issuance of some [FungibleAsset] from a
 *  server acting as an issuer (see [Issued]) of FungibleAssets.
 *
 *  It is not intended for production usage, but rather for experimentation and testing purposes where it may be
 *  useful for creation of fake assets.
 */
object IssuerFlow {
    /**
     * Issuance result from [IssuanceRequester]. It contains the issuance [SignedTransaction] of a [Cash.State] which belongs
     * to the party which requested the issuance, the [recipient]. This will be a confidential identity of the requesting party
     * generated for the transaction if [IssuanceRequester.anonymous] was specified, else it will be the well known identity.
     */
    @CordaSerializable
    data class IssuanceResult(val transaction: SignedTransaction, val recipient: AbstractParty)

    /**
     * IssuanceRequester should be used by a client to ask a remote node to issue some [FungibleAsset] with the given details.
     * Returns the transaction created by the Issuer to move the cash to the Requester.
     *
     * @param anonymous true if the issued asset should be sent to a new confidential identity, false to send it to the
     * well known identity (generally this is only used in testing).
     */
    @InitiatingFlow
    @StartableByRPC
    class IssuanceRequester(val amount: Amount<Currency>,
                            val issueTo: Party,
                            val issueToPartyRef: OpaqueBytes,
                            val issuerBank: Party,
                            val notary: Party,
                            val anonymous: Boolean) : FlowLogic<IssuanceResult>() {
        @Suspendable
        @Throws(CashException::class)
        override fun call(): IssuanceResult {
            val request = IssuanceRequest(amount, issueTo, issueToPartyRef, notary, anonymous)
            return sendAndReceive<IssuanceResult>(issuerBank, request).unwrap { result ->
                val wtx = result.transaction.tx
                val expectedAmount = Amount(amount.quantity, Issued(issuerBank.ref(issueToPartyRef), amount.token))
                val cashOutputs = wtx.filterOutputs<Cash.State> { it.owner == result.recipient }
                require(cashOutputs.size == 1) { "Require a single cash output paying ${result.recipient}, found ${wtx.outputs}" }
                require(cashOutputs.single().amount == expectedAmount) { "Require payment of $expectedAmount"}
                result
            }
        }
    }

    /**
     * Issuer refers to a Node acting as a Bank Issuer of [FungibleAsset], and processes requests from a [IssuanceRequester] client.
     * Returns the generated transaction representing the transfer of the [Issued] [FungibleAsset] to the issue requester.
     */
    @InitiatedBy(IssuanceRequester::class)
    @InitiatingFlow
    class Issuer(val requester: Party) : FlowLogic<Unit>() {
        companion object {
            object AWAITING_REQUEST : ProgressTracker.Step("Awaiting issuance request")
            object ISSUING : ProgressTracker.Step("Self issuing asset")
            object TRANSFERRING : ProgressTracker.Step("Transferring asset to issuance requester")
            object SENDING_CONFIRM : ProgressTracker.Step("Confirming asset issuance to requester")

            fun tracker() = ProgressTracker(AWAITING_REQUEST, ISSUING, TRANSFERRING, SENDING_CONFIRM)
            private val VALID_CURRENCIES = listOf(USD, GBP, EUR, CHF)
        }

        override val progressTracker: ProgressTracker = tracker()

        @Suspendable
        @Throws(CashException::class)
        override fun call() {
            progressTracker.currentStep = AWAITING_REQUEST
            val request = receive<IssuanceRequest>(requester).unwrap {
                // validate request inputs (for example, lets restrict the types of currency that can be issued)
                if (it.amount.token !in VALID_CURRENCIES) throw FlowException("Currency must be one of $VALID_CURRENCIES")
                it
            }
            // TODO: parse request to determine Asset to issue
            val result = issueCashTo(request)
            progressTracker.currentStep = SENDING_CONFIRM
            send(requester, result)
        }

        @Suspendable
        private fun issueCashTo(request: IssuanceRequest): IssuanceResult {
            // invoke Cash subflow to issue Asset
            progressTracker.currentStep = ISSUING
            val me = serviceHub.myInfo.legalIdentity
            val anonymousMe = if (request.anonymous) subFlow(SwapIdentitiesFlow(me)).ourIdentity.party else me
            val issueCashFlow = CashIssueFlow(request.amount, request.issuerPartyRef, anonymousMe, request.notary)
            val issueTx = subFlow(issueCashFlow)
            // NOTE: issueCashFlow performs a Broadcast (which stores a local copy of the txn to the ledger)
            // short-circuit when issuing to self
            if (request.issueTo == me)
                return IssuanceResult(issueTx, anonymousMe)
            // now invoke Cash subflow to Move issued assetType to issue requester
            progressTracker.currentStep = TRANSFERRING
            val anonymousIssueTo = if (request.anonymous) {
                subFlow(AskAnonymousIdentityFlow(request.issueTo))
            } else {
                request.issueTo
            }
            val moveTx = subFlow(CashPaymentFlow(request.amount, anonymousIssueTo))
            // NOTE: CashFlow PayCash calls FinalityFlow which performs a Broadcast (which stores a local copy of the txn to the ledger)
            return IssuanceResult(moveTx, anonymousIssueTo)
        }
    }

    @InitiatingFlow
    private class AskAnonymousIdentityFlow(val otherParty: Party) : FlowLogic<AnonymousParty>() {
        @Suspendable
        override fun call(): AnonymousParty = subFlow(SwapIdentitiesFlow(otherParty)).theirIdentity.party
    }

    // Unfortunately this flow is needed to automatically vend anonymous identities due to the way this cash issuance
    // workflow is designed. A better design would be to scrap IssuanceRequester.issuerBank and instead have the IssuanceRequester
    // flow execute directly on the issuerBank node. That would remove the need for this third hop.
    @InitiatedBy(AskAnonymousIdentityFlow::class)
    class VendAnonymousIdentityFlow(val otherParty: Party) : FlowLogic<Unit>() {
        @Suspendable
        override fun call() {
            subFlow(SwapIdentitiesFlow(otherParty))
        }
    }

    @CordaSerializable
    private data class IssuanceRequest(val amount: Amount<Currency>,
                                       val issueTo: Party,
                                       val issuerPartyRef: OpaqueBytes,
                                       val notary: Party,
                                       val anonymous: Boolean)
}