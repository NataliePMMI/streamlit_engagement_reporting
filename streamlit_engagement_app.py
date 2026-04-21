import re
import zipfile
from datetime import datetime
from io import BytesIO

import pandas as pd
import streamlit as st

# -----------------------------
# Streamlit page setup + styling
# -----------------------------
st.set_page_config(
    page_title="Monthly Engagement Reporting Processor",
    layout="wide",
)

st.set_option("client.toolbarMode", "minimal")
st.set_option("client.showSidebarNavigation", False)

hide_streamlit_style = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}
</style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)

BRANDS = ["PW", "HCP", "PFW", "OEM", "Mundo"]


def clone_uploaded_file(uploaded_file):
    """Create an independent in-memory copy so pandas can read it safely multiple times."""
    if uploaded_file is None:
        return None
    return BytesIO(uploaded_file.getvalue())


def read_uploaded_file(uploaded_file):
    if uploaded_file is None or not getattr(uploaded_file, "name", ""):
        raise ValueError("No file provided.")

    filename = uploaded_file.name.lower()
    file_buffer = clone_uploaded_file(uploaded_file)

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(file_buffer)
        elif filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(file_buffer, sheet_name=0)
        else:
            raise ValueError(f"Unsupported file type: {uploaded_file.name}")
    except pd.errors.EmptyDataError:
        raise ValueError(f"Empty file: {uploaded_file.name}")
    except Exception as exc:
        raise ValueError(f"Could not read file {uploaded_file.name}: {exc}")

    if df is None or df.empty:
        raise ValueError(f"Empty file: {uploaded_file.name}")

    df.columns = [str(col).strip() for col in df.columns]
    return df


def normalize_customer_id_series(series):
    return series.astype(str).str.strip()


def yes_like(value):
    if pd.isna(value):
        return False
    return str(value).strip().lower() in {"yes", "y", "true", "1"}


def derive_status(current_status, previous_status):
    current_status = "" if pd.isna(current_status) else str(current_status).strip()
    previous_status = "" if pd.isna(previous_status) else str(previous_status).strip()

    current_engaged = current_status.startswith("Engaged")
    previous_engaged = previous_status.startswith("Engaged")

    if current_engaged and previous_engaged:
        return "No change - Engaged"
    elif current_engaged and previous_status == "Unengaged":
        return "Re-engaged"
    elif current_engaged and previous_status == "#N/A":
        return "New Name"
    elif current_status == "Unengaged" and previous_engaged:
        return "Lost to Unengagement"
    elif current_status == "Unengaged" and previous_status == "Unengaged":
        return "No change - Unengaged"
    elif current_status == "Unengaged" and previous_status == "#N/A":
        return "Unknown"
    else:
        return "Unknown"


def validate_required_columns(df, required_columns, file_label, brand):
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"{brand}: Missing required columns in {file_label}: {', '.join(missing)}"
        )


def parse_brand_files(brand, uploaded_files):
    """
    Correct file roles:
    1) [Brand]_Engaged_YYYYMMDD -> engaged file, contains Customer Id only
    2) [Brand]_Email_Universe_YYYYMMDD -> email universe file, contains full email columns
    3) MMDDYY_[Brand]_EmailUniverse -> previous month report
    """
    brand_upper = brand.upper()

    engaged_matches = []
    email_universe_matches = []
    previous_month_matches = []

    engaged_pattern = re.compile(
        rf"^{re.escape(brand_upper)}_Engaged_(\d{{8}})\.(csv|xlsx|xls)$",
        re.IGNORECASE,
    )
    email_universe_pattern = re.compile(
        rf"^{re.escape(brand_upper)}_Email_Universe_(\d{{8}})\.(csv|xlsx|xls)$",
        re.IGNORECASE,
    )
    previous_month_pattern = re.compile(
        rf"^(\d{{6}})_{re.escape(brand_upper)}_EmailUniverse\.(csv|xlsx|xls)$",
        re.IGNORECASE,
    )

    for uploaded_file in uploaded_files:
        if not uploaded_file or not getattr(uploaded_file, "name", ""):
            continue

        filename = uploaded_file.name.strip()

        m = engaged_pattern.match(filename)
        if m:
            engaged_matches.append((uploaded_file, m.group(1)))
            continue

        m = email_universe_pattern.match(filename)
        if m:
            email_universe_matches.append((uploaded_file, m.group(1)))
            continue

        m = previous_month_pattern.match(filename)
        if m:
            previous_month_matches.append((uploaded_file, m.group(1)))
            continue

    if len(engaged_matches) != 1:
        raise ValueError(
            f"{brand}: Expected exactly 1 Engaged file matching "
            f"{brand}_Engaged_YYYYMMDD, found {len(engaged_matches)}."
        )

    if len(email_universe_matches) != 1:
        raise ValueError(
            f"{brand}: Expected exactly 1 Email Universe file matching "
            f"{brand}_Email_Universe_YYYYMMDD, found {len(email_universe_matches)}."
        )

    if len(previous_month_matches) != 1:
        raise ValueError(
            f"{brand}: Expected exactly 1 Previous Month Report matching "
            f"MMDDYY_{brand}_EmailUniverse, found {len(previous_month_matches)}."
        )

    engaged_file, engaged_date_str = engaged_matches[0]
    email_universe_file, email_universe_date_str = email_universe_matches[0]
    previous_month_file, _ = previous_month_matches[0]

    if engaged_date_str != email_universe_date_str:
        raise ValueError(
            f"{brand}: Engaged and Email Universe filenames must have the same YYYYMMDD "
            f"date. Found {engaged_date_str} and {email_universe_date_str}."
        )

    try:
        processing_date = datetime.strptime(engaged_date_str, "%Y%m%d")
    except ValueError:
        raise ValueError(
            f"{brand}: Invalid processing date '{engaged_date_str}' in filename."
        )

    if processing_date.day != 1:
        raise ValueError(
            f"{brand}: Processing date must be first of month (YYYYMM01). "
            f"Found {engaged_date_str}."
        )

    return {
        "engaged_file": engaged_file,
        "email_universe_file": email_universe_file,
        "previous_month_file": previous_month_file,
        "processing_date": processing_date,
    }


def sort_sheet1(df):
    def engaged_sort_key(value):
        value = "" if pd.isna(value) else str(value).strip()
        return 0 if value.startswith("Engaged") else 1

    def previous_sort_key(value):
        value = "" if pd.isna(value) else str(value).strip()
        if value.startswith("Engaged"):
            return 0
        elif value == "Unengaged":
            return 1
        elif value == "#N/A":
            return 2
        return 3

    sorted_df = df.copy()
    sorted_df["_engaged_sort"] = sorted_df["Engaged?"].apply(engaged_sort_key)
    sorted_df["_previous_sort"] = sorted_df["Previous Mo Status"].apply(previous_sort_key)

    sorted_df = sorted_df.sort_values(
        by=["_engaged_sort", "_previous_sort"],
        ascending=[True, True],
        kind="stable",
    ).drop(columns=["_engaged_sort", "_previous_sort"])

    return sorted_df


def build_cleaned_sheet(sheet1_df):
    cleaned = sheet1_df[[
        "Customer Id",
        "Email Validity Code",
        "Invalid Email",
        "Status",
    ]].copy()

    cleaned = cleaned[~cleaned["Invalid Email"].apply(yes_like)].copy()

    validity = cleaned["Email Validity Code"].fillna("").astype(str).str.strip()
    cleaned = cleaned[(validity == "") | (validity.str.startswith("V"))].copy()

    cleaned["_customer_id_norm"] = cleaned["Customer Id"].astype(str).str.strip()
    cleaned = cleaned.drop_duplicates(subset=["_customer_id_norm"], keep="first")
    cleaned = cleaned.drop(columns=["_customer_id_norm"])

    return cleaned


def build_summary_sheet(cleaned_df):
    status_order = [
        "Lost to Unengagement",
        "New Name",
        "No change - Engaged",
        "No change - Unengaged",
        "Re-engaged",
        "Unknown",
    ]

    counts = cleaned_df["Status"].value_counts().to_dict()

    rows = []
    grand_total = 0

    for status in status_order:
        count = int(counts.get(status, 0))
        rows.append({"Row Labels": status, "Count of Customer Id": count})
        grand_total += count

    rows.append({"Row Labels": "Grand Total", "Count of Customer Id": grand_total})

    return pd.DataFrame(rows)


def process_brand(brand, uploaded_files):
    try:
        parsed = parse_brand_files(brand, uploaded_files)

        engaged_file = parsed["engaged_file"]
        email_universe_file = parsed["email_universe_file"]
        previous_month_file = parsed["previous_month_file"]
        processing_date = parsed["processing_date"]

        month = processing_date.month
        year = processing_date.year
        month_label = f"{month}.1.{str(year)[-2:]}"
        engaged_label = f"Engaged {month_label}"
        file_prefix = processing_date.strftime("%m%d%y")
        sheet_date = processing_date.strftime("%Y%m%d")

        engaged_df = read_uploaded_file(engaged_file)
        email_universe_df = read_uploaded_file(email_universe_file)
        previous_month_df = read_uploaded_file(previous_month_file)

        validate_required_columns(engaged_df, ["Customer Id"], "Engaged file", brand)
        validate_required_columns(
            email_universe_df,
            [
                "Customer Id",
                "Email Address",
                "Email Validity Code",
                "Invalid Email",
                "Invalid Email Date",
            ],
            "Email Universe file",
            brand,
        )
        validate_required_columns(
            previous_month_df,
            ["Customer Id", "Engaged?"],
            "Previous Month Report",
            brand,
        )

        engaged_df = engaged_df.copy()
        email_universe_df = email_universe_df.copy()
        previous_month_df = previous_month_df.copy()

        engaged_df["Customer Id"] = normalize_customer_id_series(engaged_df["Customer Id"])
        email_universe_df["Customer Id"] = normalize_customer_id_series(
            email_universe_df["Customer Id"]
        )
        previous_month_df["Customer Id"] = normalize_customer_id_series(
            previous_month_df["Customer Id"]
        )

        email_universe_df[f"Engaged {month_label}"] = month_label

        engaged_ids = set(engaged_df["Customer Id"].dropna().tolist())
        output_df = email_universe_df.copy()

        output_df["Engaged?"] = output_df["Customer Id"].apply(
            lambda x: engaged_label if x in engaged_ids else "Unengaged"
        )

        previous_lookup = dict(
            zip(previous_month_df["Customer Id"], previous_month_df["Engaged?"])
        )
        output_df["Previous Mo Status"] = output_df["Customer Id"].map(previous_lookup)
        output_df["Previous Mo Status"] = output_df["Previous Mo Status"].fillna("#N/A")

        output_df["Status"] = output_df.apply(
            lambda row: derive_status(row["Engaged?"], row["Previous Mo Status"]),
            axis=1,
        )

        ordered_columns = [
            "Customer Id",
            "Email Address",
            "Email Validity Code",
            "Invalid Email",
            "Invalid Email Date",
            "Engaged?",
            "Previous Mo Status",
            "Status",
        ]

        sheet1_df = output_df[ordered_columns].copy()
        sheet1_df = sort_sheet1(sheet1_df)
        sheet2_df = build_cleaned_sheet(sheet1_df)
        sheet3_df = build_summary_sheet(sheet2_df)

        output = BytesIO()
        workbook_filename = f"{file_prefix}_{brand}_EmailUniverse.xlsx"
        sheet1_name = f"{brand}_Email_Universe_{sheet_date}"

        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            sheet1_df.to_excel(writer, index=False, sheet_name=sheet1_name)
            sheet2_df.to_excel(writer, index=False, sheet_name="Sheet1")
            sheet3_df.to_excel(writer, index=False, sheet_name="Sheet2", startrow=2)

        output.seek(0)
        return workbook_filename, output.getvalue(), None

    except Exception as exc:
        return None, None, str(exc)


def render_brand_examples(brand):
    brand_upper = brand.upper()
    current_example = f"{brand}_Engaged_20260401.csv"
    universe_example = f"{brand}_Email_Universe_20260401.csv"
    previous_example = f"030126_{brand_upper if brand != 'Mundo' else brand}_EmailUniverse.xlsx"

    st.caption("Expected files:")
    st.markdown(
        f"- `{current_example}`\n"
        f"- `{universe_example}`\n"
        f"- `{previous_example}`"
    )


def main():
    st.title("Monthly Engagement Reporting Processor")

    st.info(
        "Upload exactly 3 files for each brand you want to process: "
        "[Brand]_Engaged_YYYYMMDD, [Brand]_Email_Universe_YYYYMMDD, and "
        "MMDDYY_[Brand]_EmailUniverse. Only brands with uploaded files will be processed."
    )

    st.write(
        "Brands with invalid files will be listed in `ERRORS.txt` while valid brands still process."
    )

    brand_uploads = {}
    col1, col2 = st.columns(2)
    columns = [col1, col2]

    for idx, brand in enumerate(BRANDS):
        with columns[idx % 2]:
            st.subheader(brand)
            uploaded = st.file_uploader(
                f"Upload {brand} files",
                type=["csv", "xlsx", "xls"],
                accept_multiple_files=True,
                key=f"{brand}_files",
            )
            brand_uploads[brand] = uploaded
            render_brand_examples(brand)

    if st.button("Process Files", type="primary"):
        zip_buffer = BytesIO()
        errors = []
        success_count = 0

        with zipfile.ZipFile(
            zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zip_file:
            for brand in BRANDS:
                uploaded_files = brand_uploads.get(brand, [])
                non_empty_files = [
                    f for f in uploaded_files
                    if f is not None and getattr(f, "name", "").strip() != ""
                ]

                if not non_empty_files:
                    continue

                workbook_name, workbook_bytes, error = process_brand(
                    brand, non_empty_files
                )

                if error:
                    errors.append(error)
                    continue

                zip_file.writestr(workbook_name, workbook_bytes)
                success_count += 1

            if errors:
                zip_file.writestr("ERRORS.txt", "\n".join(errors))

        if success_count == 0 and not errors:
            st.error("Please upload files for at least one brand.")
            return

        zip_buffer.seek(0)

        if success_count > 0:
            st.success(f"Processed {success_count} brand(s).")

        if errors:
            st.warning("Some brands could not be processed. See ERRORS.txt in the ZIP file.")
            for err in errors:
                st.write(f"- {err}")

        st.download_button(
            label="Download Monthly_Reports.zip",
            data=zip_buffer.getvalue(),
            file_name="Monthly_Reports.zip",
            mime="application/zip",
        )

    st.caption("© 2026 PMMI Media Group. All rights reserved.")


if __name__ == "__main__":
    main()
