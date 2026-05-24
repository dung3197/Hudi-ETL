from typing import List
from enum import Enum
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, coalesce

class OGGOperationType(Enum):
    """Enum cho các loại operation trong OGG"""

    INSERT = "I"
    UPDATE = "U"
    DELETE = "D"
    INITIAL = "R"

class OGGCDCProcessor:
    def __init__(self):
        pass

    def extract_business_fields(self, df: DataFrame) -> List[str]:
        """
        Extract business field names từ before/after struct schema

        Args:
            df: Input DataFrame với structured schema

        Returns:
            List of business field names
        """
        business_fields = set()

        # Get fields từ before struct
        if "before" in df.columns:
            before_fields = df.schema["before"].dataType.fieldNames()
            business_fields.update(before_fields)

        # Get fields từ after struct
        if "after" in df.columns:
            after_fields = df.schema["after"].dataType.fieldNames()
            business_fields.update(after_fields)

        return sorted(list(business_fields))


    def flat_dataframe(self, ogg_df: DataFrame, op) -> DataFrame:
        """
        Chuyển đổi ogg dataframe thành cấu trúc flat
        """

        if not ogg_df or ogg_df.count() == 0:
            return None
        print("Begin extract all fields...")
        business_fields = self.extract_business_fields(ogg_df)
        print("Finish extract all fields...")
        print("Begin get final values...")

        # DungNT56 - chuyển type về OGGOperationType
        if op == OGGOperationType.INSERT.value or op == OGGOperationType.UPDATE.value or op == OGGOperationType.INITIAL.value:
            mapping = {f"after.{f}": f for f in business_fields}
        elif op == OGGOperationType.DELETE.value:
            mapping = {f"before.{f}": f for f in business_fields}
        else:
            return ogg_df

        select_exprs = [col(src).alias(dest) for src, dest in mapping.items()]

        other_cols = [c for c in ogg_df.columns if not (c.startswith("before.") or c.startswith("after."))]
        select_exprs.extend([col(c) for c in other_cols])

        result_df = ogg_df.select(*select_exprs)


        result_df = result_df.drop("before", "after")
        print("Finish get final values")
        return result_df
