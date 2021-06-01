package org.sunbird.incredible.pojos.ob

import com.fasterxml.jackson.annotation.JsonProperty
import org.sunbird.incredible.pojos.Gender


class Models extends Serializable {}

/**
  *
  * @param context
  * @param related     Identifies a related version of the entity.
  * @param version     The version identifier for the present edition of the entity.
  * @param endorsement A claim made about this entity.
  */
case class OBBase(@JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement)


/**
  *
  * @param id       Unique IRI for the Endorsement instance.
  *                 If using hosted verification, this should be the URI where the assertion of endorsement is accessible.
  *                 For signed Assertions, it is recommended to use a UUID in the urn:uuid namespace.
  * @param `type`
  * @param claim    An entity, identified by an id and additional properties that the endorser would like to claim about that entity.
  * @param issuer   The profile of the Endorsement’s issuer.
  * @param issuedOn Timestamp of when the endorsement was published.
  * @param context
  */
case class Endorsement(@JsonProperty("@context") context: String, related: Array[String], version: String, id: String, `type`: Array[String] = Array("Endorsement"), claim: String, issuer: Profile, issuedOn: String, verification: VerificationObject)


/**
  *
  * @param id             IRI to the awarding body, assessor or training body.
  * @param `type`
  * @param name           Name of the awarding body, assessor or training body.
  * @param email
  * @param url            URL of the awarding body, assessor or training body
  * @param description    Short description
  * @param publicKey      URLs to type CryptographicKeys
  * @param revocationList List of HTTP URLs of the signed badges that are revoked
  * @param telephone      A phone number - Part of OpenBadges, not mentioned in inCredible
  */
case class Profile(@JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement, id: String, `type`: Array[String], name: String, email: String, url: String, description: String,
                   publicKey: Array[String], revocationList: Array[String], telephone: String, verification: VerificationObject)


/**
  *
  * @param id               HTTP URL or UUID from urn:uuid namespace
  * @param `type`           Simple string "Assertion" or URLs or IRIs of current context
  * @param issuedOn         DateTime string compatible with ISO 8601 guideline For example, 2016-12-31T23:59:59+00:00
  * @param recipient
  * @param badge
  * @param image            IRI or document representing an image representing this user’s achievement.This must be a PNG or SVG image. Otherwise, use BadgeClass member.
  * @param evidence
  * @param expires          DateTime string compatible with ISO 8601 guideline,For example, 2016-12-31T23:59:59+00:00
  * @param verification
  * @param narrative        A narrative that connects multiple pieces of evidence
  * @param revoked          Defaults to false if Assertion is not referenced from a revokedAssertions list and may be omitted.
  * @param revocationReason Optional published reason for revocation, if revoked.
  */
case class Assertion(@JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement,
                     id: String, `type`: Array[String], issuedOn: String, recipient: CompositeIdentityObject, badge: BadgeClass, image: String
                     , evidence: Evidence, expires: String, verification: VerificationObject, narrative: String
                     , revoked: Boolean = false, revocationReason: Option[String])


/**
  * @param identity Value of the annotation Example: father's or a spouse's name
  * @param `type`   IRI of the property by which the recipient of a badge is identified.
  * @param hashed   Whether or not the identity value is hashed.
  * @param salt     If the recipient is hashed, this should contain the string used to salt the hash.
  */
case class IdentityObject(@JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement, identity: String, `type`: Array[String], hashed: String, salt: String)

/**
  *
  * @param context
  * @param targetName        Name of the alignment
  * @param targetURL         URL linking to the official description of the alignment target
  * @param targetDescription Short description of the alignment target
  * @param targetFramework   Name of the framework the alignment target
  * @param targetCode        If applicable, a locally unique string identifier that identifies the alignment target within its framework and/or targetUrl
  */
case class AlignmentObject(@JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement, targetName: String, targetURL: String, targetDescription: String, targetFramework: String, targetCode: String)

/**
  *
  * @param `type`               The type of verification method. Supported values for single assertion verification are HostedBadge and SignedBadge (aliases in context are available: hosted and signed)
  * @param verificationProperty The @id of the property to be used for verification that an Assertion is within the allowed scope. Only id is
  *                             * supported. Verifiers will consider id the default value if verificationProperty is omitted or if an issuer
  *                             * Profile has no explicit verification instructions, so it may be safely omitted.
  * @param startsWith           The URI fragment that the verification property must start with. Valid Assertions must have an id within this
  *                             * scope. Multiple values allowed, and Assertions will be considered valid if their id starts with one of these values.
  * @param allowedOrigins       The host registered name subcomponent of an allowed origin. Any given id URI will be considered valid.
  *                             * Refer to https://tools.ietf.org/html/rfc3986#section-3.2.2
  *                             * Example: ["example.org", "another.example.org"]
  */
case class VerificationObject(@JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement, `type`: Array[String], verificationProperty: String, startsWith: String, allowedOrigins: List[String]) extends Serializable {}

/**
  *
  * @param `type`
  * @param verificationProperty
  * @param startsWith
  * @param allowedOrigins
  * @param creator The (HTTP) id of the key used to sign the Assertion. If not present, verifiers will check public key(s) declared
  *                * in the referenced issuer Profile. If a key is declared here, it must be authorized in the issuer Profile as well.
  *                * creator is expected to be the dereferencable URI of a document that describes a CryptographicKey
  */
case class SignedVerification(@JsonProperty("@context") context: Option[String] = None, related: Option[Array[String]] = None, version: Option[String] = None, endorsement: Option[Endorsement] = None, `type`: Array[String] = Array("SignedBadge"), verificationProperty: Option[String] = None, startsWith: Option[String] = None, allowedOrigins: Option[List[String]] = None, creator: Option[String] = None)

case class HostedVerification(@JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement, `type`: Array[String] = Array("HostedBadge"), verificationProperty: String, startsWith: String, allowedOrigins: List[String])

/**
  *
  * @param id          Unique IRI for the badge - HTTP URL or URN
  * @param `type`      In most cases, this will simply be the string BadgeClass. An array including BadgeClass and other string elements that are either URLs or compact IRIs within the current context are allowed.
  * @param name        Name of the achievement represented by this class Example: "Carpentry know-how level 1"
  * @param description Description of the badge
  * @param image       HTTP URL to the image of this credential
  * @param criteria    HTTP URL to the issuer of this credential - Profile
  */
case class BadgeClass(@JsonProperty("@context") context: String, related: Option[Array[String]] = None, endorsement: Option[Endorsement] = None, id: String, `type`: Array[String] = Array("BadgeClass"), name: String, description: String, version: Option[String] = None, image: String, criteria: Criteria, issuer: Issuer, alignmentObject: Option[AlignmentObject] = None)

/**
  *
  * @param id        IRI
  * @param `type`    Type of the criteria, defaults to "Criteria"
  * @param narrative A narrative of what is needed to earn the badge
  */
case class Criteria(@JsonProperty("@context") context: Option[String] = None, related: Option[Array[String]] = None, endorsement: Option[Endorsement] = None, id: Option[String] = None, `type`: Array[String] = Array("Criteria"), narrative: String)

case class CryptographicKey(@JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement, `type`: Array[String] = Array("CryptographicKey"), id: String, owner: String, publicKeyPem: String)

/**
  *
  * @param name      Name of the awarding body, assessor or training body.
  * @param publicKey URLs to type CryptographicKeys
  */
case class Issuer(@JsonProperty("@context") context: String, related: Option[Array[String]] = None, version: Option[String] = None, endorsement: Option[Endorsement] = None, id: Option[String] = None, `type`: Array[String] = Array("Issuer"), name: String, email: Option[String] = None, url: String, publicKey: Option[Array[String]] = None)

/**
  *
  * @param id          identifies the evidence from the urn:uuid namespace or a HTTP URL of a webpage which presents the evidence
  * @param narrative   A narrative that describes the evidence and process of achievement that led to an Assertion
  * @param name        Descriptive title of the evidence
  * @param description Longer description of the evidence
  * @param genre       Describes the type of evidence, such as Certificate, Painting, Artefact, Medal, Video, Image.
  * @param audience    Description of the intended audience for a piece of evidence
  */
class Evidence(@JsonProperty("@context") context: String, related: Option[Array[String]] = None, version: Option[String] = None, endorsement: Option[Endorsement] = None, id: String, `type`: Array[String], narrative: Option[String] = None, name: String, description: Option[String] = None, genre: Option[String] = None, audience: Option[String] = None)

/**
  * @param narrative
  * @param name
  * @param description
  * @param genre
  * @param audience
  * @param subject    Subject of the evidence
  * @param assessment Assessment conducted which elicited the evidence
  * @param assessedBy HTTP URL identifier to a JSON-LD object or an embedded profile of the individual(s) or organisation(s) who assessed the competency
  * @param assessedOn DateTime when the assessment was conducted
  * @param signature
  */
case class AssessedEvidence(@JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement,
                            id: String, `type`: Array[String] = Array("Evidence", "Extension", "extensions:AssessedEvidence"), narrative: String,
                            name: String, description: String, genre: String, audience: String, subject: String, assessment: Assessment, assessedBy: String,
                            assessedOn: String, signature: Signature)

case class Assessment(@JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement, `type`: Array[String] = Array("Extension", "extensions:Assessment"), value: Float)

/**
  *
  * @param `type`
  * @param components
  * @param name
  * @param photo A HTTP URL to a printable version of this certificate.The URL points to a base64 encoded data, like data:application/pdf;base64
  *              * or data:image/jpeg;base64
  * @param dob   Date of birth in YYYY-MM-DD format.
  * @param gender
  * @param tag   Tag URI, if URN is used
  * @param urn   Uniform resource name Required in the absence of URL
  * @param url   Uniform resource locator Required in the absence of URN
  */
case class CompositeIdentityObject(@JsonProperty("@context") context: String, related: Option[Array[String]] = None, version: Option[String] = None, endorsement: Option[Endorsement] = None, identity: String, `type`: Array[String] = Array("Extension", "IdentityObject", "extensions:CompositeIdentityObject"), hashed: Boolean, salt: Option[String] = None,
                                   components: Option[List[CompositeIdentityObject]] = None,
                                   name: String,
                                   photo: Option[String] = None, dob: Option[String] = None, gender: Option[Gender] = None, tag: Option[String] = None, urn: Option[String] = None, url: Option[String] = None)

/**
  *
  * @param creator        Who created the signature?
  * @param created        Date timestamp of when the signature was created
  * @param signatureValue The signature hash of the certificate
  */
case class Signature(`type`: String = "LinkedDataSignature2015", creator: String, created: String, signatureValue: String)

/**
  * An extension to Assertion class
  */
case class CertificateExtension(@JsonProperty("@context") context: String, related: Option[Array[String]] = None, version: Option[String] = None, endorsement: Option[Endorsement] = None,
                                id: String, issuedOn: String, recipient: CompositeIdentityObject, badge: BadgeClass, image: Option[String] = None
                                , var evidence: Option[TrainingEvidence] = None, expires: String, verification: Option[VerificationObject] = None, narrative: Option[String] = None
                                , revoked: Boolean = false, revocationReason: Option[String] = None, `type`: Array[String] = Array("Assertion", "Extension", "extensions:CertificateExtension"),
                                value: Option[Float] = None, awardedThrough: Option[String] = None, signatory: Array[SignatoryExtension],
                                var printUri: Option[String] = None, validFrom: String, var signature: Option[Signature] = None)

/**
  *
  * @param designation Designation or capacity of the signatory
  * @param image       HTTP URL or a data URI for an image associated with the signatory
  * @param publicKey
  * @param name
  */
case class SignatoryExtension(@JsonProperty("@context") context: String, related: Option[Array[String]] = None, version: Option[String] = None, endorsement: Option[Endorsement] = None, identity: String, `type`: Array[String] = Array("Extension", "extensions:SignatoryExtension"), hashed: Option[String] = None, salt: Option[String] = None, designation: String, image: String, publicKey: Option[CryptographicKey] = None, name: String)


case class TrainingEvidence(@JsonProperty("@context") context: String, related: Option[Array[String]] = None,
                            version: Option[String] = None, endorsement: Option[Endorsement] = None, id: String,
                            `type`: Array[String] = Array("Evidence", "Extension", "extensions:TrainingEvidence"),
                            narrative: Option[String] = None, name: String, description: Option[String] = None,
                            genre: Option[String] = None, audience: Option[String] = None, subject: Option[String] = None
                            , trainedBy: Option[String] = None, duration: Option[Duration] = None, session: Option[String] = None)

case class Duration(startDate: String, endDate: String)

case class MarksAssessment(minValue: Float, maxValue: Float, passValue: Float, @JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement, `type`: Array[String] = Array("Extension", "extensions:MarksAssessment"), value: Float)

case class RankAssessment(maxValue: Float, @JsonProperty("@context") context: String, related: Array[String], version: String, endorsement: Endorsement, `type`: Array[String] = Array("Extension", "extensions:RankAssessment"), value: Float)
