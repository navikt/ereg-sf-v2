package no.nav.ereg.salesforce

import com.google.gson.reflect.TypeToken
import mu.KotlinLogging
import org.http4k.core.Response
import org.http4k.core.Status
import java.lang.reflect.Type

/**
 * Please refer to
 * https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_composite_sobjects_collections_create.htm
 */

private val log = KotlinLogging.logger { }

data class SFsObjectStatus(
    val id: String = "",
    val success: Boolean,
    val errors: List<SFObjectError> = emptyList(),
)

data class SFObjectError(
    val statusCode: String,
    val message: String,
    val fields: List<String> = emptyList(),
)

/*

val body =
                SFsObjectRest(
                    records =
                        orgObjects.map {
                            if (samples > 0) {
                                File(
                                    "/tmp/samples",
                                ).appendText(
                                    "KEY: ${it.first.key.orgNumber}#${it.first.key.orgType}#${it.first.value.jsonHashCode}\nVALUE:${it.first.value.orgAsJson}\n\n",
                                )
                                samples--
                            }
                            KafkaMessage(
                                CRM_Topic__c = topic,
                                CRM_Key__c = "${it.first.key.orgNumber}#${it.first.key.orgType}#${it.first.value.jsonHashCode}",
                                CRM_Value__c = (it.first.value.orgAsJson as String).encodeB64(),
                            )
                        } +
                            orgTombStones.map {
                                KafkaMessage(
                                    CRM_Topic__c = topicKafkaTombstones,
                                    CRM_Key__c = "${it.first.key.orgNumber}",
                                    CRM_Value__c = "${it.first.key.orgNumber}",
                                )
                            },
                ).toJson()

                internal data class OrgObjectTombstone(
    val key: EregOrganisationEventKey,
) : OrgObjectBase()

internal data class OrgObject(
    val key: EregOrganisationEventKey,
    val value: EregOrganisationEventValue,
) : OrgObjectBase()

message EregOrganisationEventKey {

  string org_number = 1;

  enum OrgType {
    ENHET = 0;
    UNDERENHET = 1;
  }
  OrgType org_type = 2;
}

// this message will be the value part of kafka payload
message EregOrganisationEventValue {

  string org_as_json = 1;
  int32 json_hash_code = 2;
}
 */
