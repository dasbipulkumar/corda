package net.corda.core.schemas.converters

import net.corda.core.identity.AbstractParty
import net.corda.core.node.services.IdentityService
import org.bouncycastle.asn1.x500.X500Name
import javax.persistence.AttributeConverter
import javax.persistence.Converter

/**
 * Converter to persist a party as its's well known identity (where resolvable)
 * Completely anonymous parties are stored as null (to preserve privacy)
 */
@Converter(autoApply = true)
class AbstractPartyToX500NameAsStringConverter(identitySvc: () -> IdentityService) : AttributeConverter<AbstractParty, String> {

    private val identityService: IdentityService by lazy {
        identitySvc()
    }

    override fun convertToDatabaseColumn(party: AbstractParty?): String? {
        party?.let {
            return identityService.partyFromAnonymous(party)?.toString()
        }
        return null // non resolvable anonymous parties
    }

    override fun convertToEntityAttribute(dbData: String?): AbstractParty? {
        dbData?.let {
            val party = identityService.partyFromX500Name(X500Name(dbData))
            return party as AbstractParty
        }
        return null // non resolvable anonymous parties are stored as nulls
    }
}
