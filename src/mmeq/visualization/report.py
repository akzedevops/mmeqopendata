import logging
import os
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

try:
    from fpdf import FPDF
    HAS_FPDF = True
except ImportError:
    HAS_FPDF = False


class ReportPDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 10, "Myanmar Earthquake & Dam Risk Report", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(3)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def section_title(self, title: str):
        self.set_font("Helvetica", "B", 12)
        self.set_fill_color(41, 128, 185)
        self.set_text_color(255, 255, 255)
        self.cell(0, 8, f"  {title}", fill=True, new_x="LMARGIN", new_y="NEXT")
        self.set_text_color(0, 0, 0)
        self.ln(3)

    def body_text(self, text: str):
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 5, text)
        self.ln(2)

    def key_stat(self, label: str, value: str):
        self.set_font("Helvetica", "B", 10)
        self.cell(70, 6, f"  {label}:", new_x="END")
        self.set_font("Helvetica", "", 10)
        self.cell(0, 6, str(value), new_x="LMARGIN", new_y="NEXT")

    def add_table(self, headers: list, rows: list, col_widths: list = None):
        if col_widths is None:
            col_widths = [190 / len(headers)] * len(headers)

        self.set_font("Helvetica", "B", 8)
        self.set_fill_color(220, 220, 220)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 6, str(h)[:25], border=1, fill=True, new_x="END")
        self.ln()

        self.set_font("Helvetica", "", 7)
        for row in rows:
            for i, val in enumerate(row):
                self.cell(col_widths[i], 5, str(val)[:25], border=1, new_x="END")
            self.ln()

    def add_image_if_exists(self, path: str, w: int = 180):
        if os.path.exists(path):
            self.image(path, w=w)
            self.ln(3)


def generate_pdf_report(
    df: pd.DataFrame,
    risk_df: Optional[pd.DataFrame] = None,
    forecast_params: Optional[dict] = None,
    b_value: float = 0,
    a_value: float = 0,
    mc: float = 0,
    output_path: str = "myanmar_earthquake_report.pdf",
    map_image: Optional[str] = None,
    cluster_image: Optional[str] = None,
) -> str:
    if not HAS_FPDF:
        logger.error("fpdf2 required for PDF generation")
        return ""

    pdf = ReportPDF()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=20)

    df = df.copy()
    df["time_utc"] = pd.to_datetime(df["time_utc"])
    start = df["time_utc"].min().strftime("%Y-%m-%d")
    end = df["time_utc"].max().strftime("%Y-%m-%d")

    pdf.section_title("1. Executive Summary")
    pdf.body_text(
        f"This report covers {len(df):,} earthquake events in Myanmar from {start} to {end}. "
        f"The largest event was M{df['mag'].max():.1f} on {df.loc[df['mag'].idxmax(), 'time_utc'].strftime('%Y-%m-%d')}. "
        f"The Gutenberg-Richter b-value is {b_value:.3f} with magnitude of completeness Mc={mc:.1f}."
    )
    pdf.ln(2)
    pdf.key_stat("Total Events", f"{len(df):,}")
    pdf.key_stat("Date Range", f"{start} to {end}")
    pdf.key_stat("Max Magnitude", f"M{df['mag'].max():.1f}")
    pdf.key_stat("Mean Depth", f"{df['depth'].mean():.1f} km")
    pdf.key_stat("b-value", f"{b_value:.3f}")
    pdf.key_stat("a-value", f"{a_value:.3f}")
    pdf.key_stat("Mc (completeness)", f"{mc:.1f}")
    pdf.ln(4)

    if map_image:
        pdf.section_title("2. Earthquake Map")
        pdf.add_image_if_exists(map_image)
        pdf.add_page()

    if cluster_image:
        pdf.section_title("3. Seismic Clustering Analysis")
        pdf.add_image_if_exists(cluster_image)
        pdf.add_page()

    if risk_df is not None and not risk_df.empty:
        pdf.section_title("4. Dam Seismic Risk Assessment")
        pdf.body_text(
            f"Risk assessment for {len(risk_df)} dams using PGA estimation from the M{forecast_params.get('mainshock_mag', '?')} mainshock, "
            f"fault proximity, and structural exposure scoring."
        )

        for grade in ["Critical", "High", "Moderate", "Low"]:
            count = (risk_df["risk_grade"] == grade).sum()
            pdf.key_stat(f"  {grade} Risk", f"{count} dams")
        pdf.ln(3)

        pdf.set_font("Helvetica", "B", 10)
        pdf.cell(0, 6, "Top 30 Highest Risk Dams:", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        headers = ["Name", "River", "PGA(g)", "Dist Fault", "Risk", "Grade"]
        widths = [50, 35, 25, 25, 25, 30]
        top = risk_df.head(30)
        rows = []
        for _, r in top.iterrows():
            rows.append([
                r["name"][:22],
                str(r.get("river", ""))[:15],
                f"{r['pga_g']:.4f}",
                f"{r.get('dist_to_fault_km', '?')} km",
                f"{r['composite_risk']:.1f}",
                r["risk_grade"],
            ])
        pdf.add_table(headers, rows, widths)
        pdf.add_page()

    if forecast_params:
        pdf.section_title("5. Aftershock Forecast")
        pdf.body_text(
            f"Modified Omori law parameters fitted to aftershocks of M{forecast_params.get('mainshock_mag', '?')} mainshock."
        )
        pdf.key_stat("K (productivity)", f"{forecast_params.get('K', 0):.2f}")
        pdf.key_stat("c (time offset)", f"{forecast_params.get('c_hours', 0):.3f} hours")
        pdf.key_stat("p (decay exponent)", f"{forecast_params.get('p', 0):.3f}")
        pdf.key_stat("Expected M>=3 (7d)", f"{forecast_params.get('expected_aftershocks_7d', 0):.0f}")
        pdf.key_stat("Expected M>=3 (30d)", f"{forecast_params.get('expected_aftershocks_30d', 0):.0f}")
        pdf.key_stat("Expected M>=3 (90d)", f"{forecast_params.get('expected_aftershocks_90d', 0):.0f}")
        pdf.ln(3)
        pdf.body_text(
            "Note: These are statistical estimates based on the Omori decay law. "
            "Actual aftershock rates may vary due to local geological conditions."
        )

    pdf.section_title("6. Data Sources")
    pdf.body_text(
        "- Earthquake data: mmeq.akze.net API (USGS/ISC sourced)\n"
        "- Dam data: Open Development Mekong (IFC/WLE), CC BY-SA 4.0\n"
        "- Fault lines: USGS/Plate boundary data\n"
        "- PGA estimation: Boore & Joyner (1997) attenuation model\n"
        "- Report generated by mmeq-opendata toolkit"
    )

    pdf.output(output_path)
    logger.info(f"PDF report saved -> {output_path}")
    return output_path
