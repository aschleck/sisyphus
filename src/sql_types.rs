use sqlx::{
    Any, Database, Decode, Encode, Type,
    any::{AnyTypeInfo, AnyTypeInfoKind},
    encode::IsNull,
    error::BoxDynError,
};
use sqlx_core::any::AnyValueKind;
use time::OffsetDateTime;

// Stored with millisecond precision
pub(crate) struct DecodableOffsetDateTime(pub OffsetDateTime);

impl Type<Any> for DecodableOffsetDateTime {
    fn type_info() -> AnyTypeInfo {
        AnyTypeInfo {
            kind: AnyTypeInfoKind::BigInt,
        }
    }

    fn compatible(ty: &AnyTypeInfo) -> bool {
        ty.kind().is_integer()
    }
}

impl<'r> Decode<'r, Any> for DecodableOffsetDateTime {
    fn decode(value: <Any as Database>::ValueRef<'r>) -> Result<Self, BoxDynError> {
        let value = <i64 as Decode<Any>>::decode(value)?;
        let dt = OffsetDateTime::from_unix_timestamp(value / 1000)?
            .replace_millisecond((value % 1000) as u16)?;
        Ok(DecodableOffsetDateTime(dt))
    }
}

impl<'q> Encode<'q, Any> for DecodableOffsetDateTime {
    fn encode_by_ref(
        &self,
        buf: &mut <Any as Database>::ArgumentBuffer<'q>,
    ) -> Result<IsNull, BoxDynError> {
        buf.0.push(AnyValueKind::BigInt(self.0.unix_timestamp()));
        Ok(IsNull::No)
    }
}
