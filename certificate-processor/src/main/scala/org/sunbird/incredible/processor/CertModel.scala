package org.sunbird.incredible.processor

import org.sunbird.incredible.pojos.ob.{Criteria, Issuer, SignatoryExtension}

case class CertModel(courseName: String, recipientName: String, recipientId: String, recipientEmail: String, recipientPhone: String
                     , certificateName: String, certificateDescription: String, certificateLogo: String, issuedDate: String, issuer: Issuer,
                     validFrom: String, expiry: String, signatoryList: Array[SignatoryExtension], assessedOn: String, identifier: String, criteria: Criteria)
