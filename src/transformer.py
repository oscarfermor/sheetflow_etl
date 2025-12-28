# transformer.py
import pandas as pd
from typing import List
from logger import logger
from datetime import datetime, timezone


class BronzeToSilverTransformer:
    """
    Transforms raw (bronze) data into cleaned (silver) data.
    Applies data quality rules, standardization, and basic transformations.
    """

    def __init__(self):
        self.transformations_applied = []
        self.processed_at = datetime.now(timezone.utc)

    def transform(self, df: pd.DataFrame, worksheet_name: str) -> pd.DataFrame:
        """
        Main transformation pipeline for a single worksheet.

        Args:
            df: Raw dataframe from bronze layer
            worksheet_name: Name of the worksheet (for context-specific transformations)

        Returns:
            Transformed dataframe ready for silver layer
        """
        if df.empty:
            logger.warning(
                f"Worksheet {worksheet_name} is empty. Skipping transformation."
            )
            return df
        logger.info(f"Starting transformation for worksheet: {worksheet_name}")
        self.transformations_applied = []

        # Create a copy to avoid modifying original
        df_transformed = df.copy()

        # Apply transformations in sequence
        df_transformed = self._add_headers(df_transformed)
        df_transformed = self._remove_empty_rows(df_transformed)
        df_transformed = self._standardize_column_names(df_transformed)
        df_transformed = self._handle_missing_values(df_transformed)
        df_transformed = self._cast_data_types(df_transformed, worksheet_name)
        df_transformed = self._deduplicate(df_transformed)
        df_transformed = self._add_metadata(
            df_transformed, worksheet_name, processed_at=self.processed_at
        )

        logger.info(
            f"Transformation completed for {worksheet_name}. "
            f"Rows: {len(df)} -> {len(df_transformed)}"
        )

        return df_transformed

    def _add_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Use first row as headers if not already done."""
        if df.empty:
            return df
        raw_headers = df.iloc[0]

        headers = []
        seen = set()
        for i, header in enumerate(raw_headers):
            if pd.isna(header) or str(header).strip() == "":
                name = f"col_{i+1}"
            else:
                name = str(header).strip()

            if name in seen:
                suffix = 1
                while f"{name}_{suffix}" in seen:
                    suffix += 1
                name = f"{name}_{suffix}"
            headers.append(name)
            seen.add(name)

        df = df[1:].reset_index(drop=True)
        df.columns = headers
        self.transformations_applied.append("headers_added")
        return df

    def _remove_empty_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove rows that are completely empty."""
        if df.empty:
            return df
        initial_rows = len(df)
        df = df.dropna(how="all").reset_index(drop=True)
        removed = initial_rows - len(df)
        if removed > 0:
            logger.debug(f"Removed {removed} empty rows")
            self.transformations_applied.append(f"removed_{removed}_empty_rows")
        else:
            logger.debug("No empty rows found")
        return df

    def _standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names: lowercase, replace spaces with underscores."""
        if df.empty:
            return df
        standardized = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("[^a-z0-9_]", "", regex=True)
        )

        # Following logic to ensure valid column names after standardization:
        # Duplicate column names
        # Columns that become empty
        # Columns starting with numbers

        # Handle empty column names
        standardized = [
            col if col else f"col_{i}" for i, col in enumerate(standardized)
        ]

        # Handle columns starting with numbers
        standardized = [f"_{col}" if col[0].isdigit() else col for col in standardized]

        # Ensure uniqueness
        seen = {}
        final_cols = []
        for col in standardized:
            if col not in seen:
                seen[col] = 0
                final_cols.append(col)
            else:
                seen[col] += 1
                final_cols.append(f"{col}_{seen[col]}")

        df.columns = final_cols
        self.transformations_applied.append("columns_standardized")
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values based on column type."""
        if df.empty:
            return df

        # Normalize empty strings and whitespace-only strings
        df = df.replace(r"^\s*$", pd.NA, regex=True)

        self.transformations_applied.append("missing_values_normalized")
        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows."""
        if df.empty:
            return df
        initial_rows = len(df)

        # Keep first occurrence, reset index afterwards
        df = df.drop_duplicates(keep="first").reset_index(drop=True)
        removed = initial_rows - len(df)
        if removed > 0:
            logger.warning(f"Removed {removed} duplicate rows")
            self.transformations_applied.append(f"removed_{removed}_duplicates")
        else:
            logger.info("No duplicate rows found")
        return df

    def _cast_data_types(self, df: pd.DataFrame, worksheet_name: str) -> pd.DataFrame:
        """Cast columns to appropriate data types."""
        if df.empty:
            return df

        for col in df.columns:
            converted = pd.to_numeric(df[col], errors="coerce")
            non_null_ratio = converted.notna().mean()

            if non_null_ratio > 0.9:
                df[col] = converted
                logger.debug(
                    f"Casted column {col} to numeric in worksheet {worksheet_name}"
                )
            else:
                logger.debug(
                    f"Column {col} in worksheet {worksheet_name} not casted to numeric (non-null ratio: {non_null_ratio:.2f})"
                )

        self.transformations_applied.append("data_types_cast")
        return df

    def _add_metadata(
        self,
        df: pd.DataFrame,
        worksheet_name: str,
        processed_at: datetime | None = None,
    ) -> pd.DataFrame:
        """Add metadata columns for traceability."""
        if df.empty:
            return df

        if processed_at is None:
            processed_at = self.processed_at

        df["_worksheet_source"] = worksheet_name
        df["_processed_at"] = processed_at
        df["_ingestion_date"] = processed_at.date().isoformat()

        self.transformations_applied.append("metadata_added")
        return df

    def get_transformation_summary(self) -> List[str]:
        """Return list of transformations applied."""
        return self.transformations_applied
