from langchain_experimental.agents import create_pandas_dataframe_agent
from langchain_openai import ChatOpenAI
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from io import StringIO
import os
import re

plt.rcParams["font.family"] = 'NanumGothic'
plt.rcParams['axes.unicode_minus'] = False

llm = ChatOpenAI(
    base_url="",
    api_key="not-needed",
    model="Qwen3-14B",
    max_tokens=5000,
)

design_prompt = """
Follow all instructions below **strictly**, and always assume that the original data and all column names are in **Korean**.
**Do not translate, alter, or abbreviate any Korean column names**. Use them **as-is** in the code.
However, for all visualization labels (titles, axes, legends), replace underscores (`_`) with natural **Korean spacing**.

1. Graph Type Selection:
   You MUST select the chart type strictly based on the user's intent.  
   Start by asking: **"What do you want to show?"**  
   Then, follow the decision logic and thresholds below:

   - Distribution:
      - Single variable:
        - If number of items <= 10 → Vertical bar chart
        - If number of items > 10 → Line histogram or Smoothed line histogram
      - Two variables → Scatter plot
      - Three variables → Area plot

   - Composition:
      - Proportions of a whole:
        - If number of categories <= 6 → Pie chart
      - 100% comparison over time or groups → 100% Stacked bar chart or 100% Stacked area chart
      - Absolute group totals → Stacked bar chart
      - Total accumulation → Waterfall chart

   - Comparison:
      - Non-time-series data:
        - If number of items <= 10 → Vertical bar chart
        - If number of items > 10 → Horizontal bar chart
      - Time-series data:
        - If number of time points <= 12 → Line plot
        - If number of time points > 12 → Multi-line plot or Smoothed area chart

   - Relationship:
      - Two variables → Scatter plot
      - Three variables → Bubble chart

   - Special Cases:
      - Grouped variables by category:
        - If number of groups <= 10 → Grouped vertical bar chart
        - If number of groups > 10 → Grouped horizontal bar chart
      - Matrix-style tables → Heatmap or Color matrix

   - IMPORTANT:
      Do NOT guess the chart type. Always follow this logic tree exactly, using the specified numeric thresholds.


2. Text Formatting:

   - You MUST NOT set or display any chart title.
     - Do NOT include any line like:
       ax.set_title(...)
     - The chart must appear **without any title**, even if a subject is provided.

   - All axis labels (`xlabel`, `ylabel`) and legends must be written in **Korean only**.
     - Do **NOT** use English or mixed-language text.
     - Automatically convert any underscores (`_`) into natural **Korean spacing** (e.g., `인구_수` → `인구 수`).

   - Axis labels (`ax.set_xlabel`, `ax.set_ylabel`) must clearly describe the column's meaning in **Korean**, with appropriate spacing and font size.

     - Always compute `base_height` right after `fig, ax = plt.subplots(...)`:
       base_height = fig.get_size_inches()[1]

     - Set all font sizes relative to `base_height`:
       ax.set_xlabel("X축 이름", fontsize=base_height * 1.8)
       ax.set_ylabel("Y축 이름", fontsize=base_height * 1.8)
       ax.tick_params(labelsize=base_height * 0.9)

   - Legends (`ax.legend`) must be shown if applicable.
     - The legend text must be written in Korean and follow natural spacing (i.e., replace `_` with space).
     - Font size must be set relative to `base_height`, and position should avoid overlap:
       ax.legend(fontsize=base_height * 1.2, loc='best')

   - All text must be legible but not overwhelming — scale all font sizes **proportionally to the chart size** to maintain clarity and consistency.


3. Graph Composition:
  - Axis labels must accurately describe the column meanings.
  - Legends must be clean, structured, and easy to distinguish.
  - Use All Data:
    - You must always use the **entire dataset** for plotting.
    - Do NOT slice, filter, or drop rows unless explicitly instructed.
    - To improve readability, sort the data if appropriate.

  - For numeric x-axis:

    - IMPORTANT: If x-axis values represent **years or dates**, they must NEVER appear as floats.
      - You MUST NOT show values like `2010.0`, `2010.5`, or `2011.5` under any circumstances.
      - You MUST ensure that only the actual values present in the **original data** (e.g., `2010`, `2011`, `2012`, ...) are shown.
      - Suppress any interpolated, fractional, or automatically inserted values completely.

      - You MUST also remove the decimal point even if the value is a float with `.0`. Always display it as an **integer string** like `"2010"`.

      - Example:
        ```python
        xticks = df['years'].values
        labels = [str(int(tick)) if tick in year_values else "" for tick in xticks]
        ax.set_xticks(xticks)
        ax.set_xticklabels(labels)
        ```

4. Style & Theme:
   - Use **large, readable font sizes** for all text.
   - Use `tight_layout()` to prevent overlap of elements.
   - You MUST NEVER write any code that changes fonts or themes:
     - Forbidden: `matplotlib.rcParams`, `plt.rc`, `matplotlib.font_manager`, `fontdict`
     - Also forbidden: `seaborn.set_theme()`, `seaborn.set_style()`, or any font logic.


5. Seaborn + Missing Data Handling:
   - Always use **seaborn** together with **matplotlib**.
   - Drop or ignore NA values as needed.
   - Do NOT fill missing values unless explicitly instructed.


6. Visual Enhancements:

   - **Rendering Quality**:
     - All plots must use high resolution to avoid blurry or low-quality output:
       ```python
       plt.figure(dpi=200)
       ```
       or
       ```python
       fig = plt.figure(dpi=200)
       ax = fig.add_subplot()
       ```

   - **Figure Size (Dynamic)**:
     - For **most plots**, dynamically adjust the figure size based on the number of x-axis values:
       ```python
       figsize = (max(6, len(x) * 1.0), max(6, len(x) * 0.8))
       fig, ax = plt.subplots(figsize=figsize, dpi=200)
       ```
     - This ensures wide plots like bar and line plots scale appropriately.

     - For **pie charts**, always use a fixed square size for visual balance:
       ```python
       fig, ax = plt.subplots(figsize=(6, 6), dpi=200)
       ```
     - Do **not** apply dynamic sizing to pie charts.

   - **Time-series X-axis Label Cleanup**:
     - If x-axis values are years or dates and show decimals (e.g., 2015.0, 2015.5), round them to integers:
       ```python
       ax.set_xticklabels([int(label.get_text()) for label in ax.get_xticklabels()])
       ```연도'].astype(str)


   - **Grid Layering**:
     - Grid must always appear **behind** the data elements:
       ```python
       plt.grid(True, zorder=0, color='gray', alpha=0.3)
       ```
     - All data elements (e.g., bars, lines, markers) **must be explicitly set to** `zorder=2`:
       - Example:
         ```python
         sns.barplot(..., zorder=2)
         ax.plot(..., zorder=2)
         ax.scatter(..., zorder=2)
         ```

   - **Numeric Formatting**:
     - Format **x-axis ticks only** with comma separators **when values represent numeric data (e.g., population)**.
     - For horizontal bar charts (where x-axis is numeric):
       ```python
       from matplotlib.ticker import FuncFormatter
       ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}"))
       ```
     - **Do NOT format the y-axis**, since it contains categorical labels (e.g., city names).
       - Explicitly avoid:
         ```python
         ax.yaxis.set_major_formatter(...)
         ```
     - For vertical bar charts or line charts, apply formatting to the y-axis instead:
     ```python
     ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}"))
     ```


   - **Data Value Labels** (MANDATORY):
     - All bars, points, or markers must display their numeric value as a label **just above the data point**.
     - Use **only black or white** for the label color (`color='black'` or `'white'`) to ensure contrast.
     - Align the label vertically using `va='bottom'` or `va='top'` as appropriate.
     - Font size must scale with figure size. Define:
       ```python
       base_height = fig.get_size_inches()[1]
       value_annotation_fontsize = base_height * 0.8
       ```

     - Use **y-axis range-based offset** to adjust label height:
       ```python
       ymin, ymax = ax.get_ylim()
       offset = (ymax - ymin) * 0.01
       ```

     - For **vertical bar plots**, adjust label position dynamically:
       ```python
       for bar in ax.patches:
           height = bar.get_height()
           if height >= 0:
               y = height + offset
               va = 'bottom'
           else:
               y = height - offset
               va = 'top'
           ax.text(
               bar.get_x() + bar.get_width() / 2,
               y,
               f"{height:.1f}%",
               ha='center',
               va=va,
               fontsize=value_annotation_fontsize,
               zorder=4,
               color='black',
               fontweight='bold'
           )
       ```

     - For **line plots**:
       ```python
       for xi, yi in zip(x, y):
           if pd.notna(yi):
               ax.text(
                   xi,
                   yi + offset,
                   f"{yi:,.0f}",
                   ha='center',
                   va='bottom',
                   fontsize=value_annotation_fontsize,
                   zorder=4,
                   color='black',
                   fontweight='bold'
               )
       ```

        - **Line Plot Point Markers**:
           - When drawing line plots (e.g., `sns.lineplot(...)`), you MUST also display **circular point markers** for each data point.
           - Use the argument:
             ```python
             sns.lineplot(..., marker='o', ...)
             ```
           - This ensures each data value is clearly represented both as a point and as part of the line.


     - For **horizontal bar plots** (i.e., `sns.barplot(..., orient='h')`):

        - Seaborn determines orientation based on the axis data types:
          - If `x` is numeric and `y` is categorical → horizontal bar chart
          - If `x` is categorical and `y` is numeric → vertical bar chart

        - Recommended examples:
          ```python
          sns.barplot(x='value_column', y='category_column', data=df)
          # OR explicitly:
          sns.barplot(x='value_column', y='category_column', data=df, orient='h')
          ```

        - Avoid ambiguity by explicitly setting `orient='h'` if direction matters

        - Apply horizontal offset using:
          ```python
          xmin, xmax = ax.get_xlim()
          offset = (xmax - xmin) * 0.01
          ```

        - Format only the x-axis (which contains numeric values):
          ```python
          from matplotlib.ticker import FuncFormatter
          ax.xaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{x:,.0f}"))
          ```

        - **Do NOT format y-axis ticks**; skip this line:
          ```python
          # ax.yaxis.set_major_formatter(...) ← not allowed
          ```

        - Annotate value labels using:
          ```python
          for bar in ax.patches:
              width = bar.get_width()
              ax.text(
                  width + offset,
                  bar.get_y() + bar.get_height() / 2,
                  f"{width:,.0f}",
                  va='center',
                  ha='left',
                  fontsize=value_annotation_fontsize,
                  zorder=4,
                  color='black',
                  fontweight='bold'
              )
          ```
          
     - For **Pie Charts**:
       - Pie charts must always:
         - Be **sorted by value in descending order** before plotting.
         - Start from the **top (0 degrees / 12 o'clock position)**:
           ```python
           startangle=90
           ```
         - Be arranged in a **counterclockwise direction**:
           ```python
           counterclock=True
           ```
         - MUST be rendered as a **full-circle pie chart (not a donut)**:
           - Do **not** use `wedgeprops={'width': ...}` or set `radius` to values that cut out the center.
           - Ensure **default pie layout** is used:
             ```python
             df_sorted = df.sort_values(by="value_column", ascending=False)
             plt.pie(
                 df_sorted["value_column"],
                 labels=df_sorted["label_column"],
                 startangle=90,
                 counterclock=True,
                 autopct="%1.1f%%"
             )
             ```

   - **Color Palette**:
     - All plots must use aesthetically pleasing and readable **pastel-style colors**.
     - Use one of the following official pastel colormaps from Matplotlib:
       - "Set1"
     
     - For plots with **multiple categories or groups**, you MUST explicitly set the pastel color palette:
       colors = plt.get_cmap("Set1").colors
       sns.barplot(..., palette=colors)
       plt.pie(..., colors=colors)

     - For **single-line plots** (i.e., sns.lineplot() with only one line),  
       you must **ABSOLUTELY NEVER specify a color palette or color**.
       - Do NOT use `palette=...`  
       - Do NOT use `color=...`
       - Doing so will result in **rejection of the output**.

       - You must allow Seaborn to automatically assign its default color for visual clarity.

       - This rule ensures visual consistency and avoids overriding Seaborn's internal styling logic for single-line plots.

       - Example (correct):
         ```python
         sns.lineplot(x='연도', y='학생수', data=df, marker='o', zorder=2)
         ```

       - NEVER do this:
         ```python
         sns.lineplot(x='연도', y='학생수', data=df, marker='o', zorder=2, color='blue')  # ← Forbidden!
         ```


7. Plot Box Border:
   - All 4 borders of the plot must be visible:
     ```python
     for spine in ax.spines.values():
         spine.set_visible(True)
         spine.set_linewidth(0.8)
         spine.set_zorder(3)
     ```

8. Bar Width Control:
   - If using `sns.barplot()`, control bar thickness:
     ```python
     sns.barplot(..., width=0.6)
     ```

**Finally, you MUST include the full Python code that generates the plot.  
The answer is NOT complete unless it includes the entire visualization code block.**
"""

def extract_clean_code(llm_output: str, df_assign_code: str = "df = df_table[i]") -> str:
    parts = llm_output.split("```")

    for part in parts:
        cleaned = part.strip()
        if cleaned.startswith("python\nimport") or cleaned.startswith("import"):
            code = cleaned.replace("python\n", "").rstrip("`").strip()

            # 1. 폰트 및 테마 관련 코드 제거
            code = re.sub(r"(?i)^.*(rcParams|mpl\.rc|font_manager|set_theme|set_style).*$", "", code, flags=re.MULTILINE)

            # 2. 줄 단위 분리
            lines = code.splitlines()

            # 3. import 문 모으기
            import_lines = [line for line in lines if line.strip().startswith("import") or line.strip().startswith("from")]
            rest_lines = [line for line in lines if line not in import_lines]

            # 4. df 및 폰트 설정 삽입
            custom_lines = [
                df_assign_code,
                'plt.rcParams["font.family"] = "NanumGothic"',
                'plt.rcParams["axes.unicode_minus"] = False',
            ]

            final_lines = import_lines + [""] + custom_lines + [""] + rest_lines
            return "\n".join(final_lines)

    # fallback 제거 → 응답 재시도 유도
    raise ValueError("⚠️ 코드 블록을 찾을 수 없습니다. 응답 재생성이 필요합니다.")


def ensure_save_and_show(code_str: str, name: str = None, directory: str = "./data"):
    os.makedirs(directory, exist_ok=True)

    filename = f"{name}.png"
    full_path = os.path.abspath(os.path.join(directory, filename))

    # 기존 코드에서 show와 save 처리
    has_show = "plt.show()" in code_str
    has_save = "plt.savefig" in code_str

    code_str = re.sub(r"plt\.show\(\)", "", code_str)

    if has_save:
        code_str = re.sub(r"plt\.savefig\(.*?\)", f"plt.savefig('{full_path}')", code_str)
    else:
        code_str += f"\nplt.savefig('{full_path}')"

    code_str += "\nplt.show()"

    return code_str.strip(), full_path


def run_graph_generation(df_table, table_name):
    print(f"[graph.py] start, {df_table}, {table_name}")
    MAX_RETRIES = 3

    for i in range(len(table_name)):
        agent = create_pandas_dataframe_agent(
            llm,
            df_table[i],
            verbose=False,
            allow_dangerous_code=True
        )

        query = table_name[i]
        full_query = design_prompt.strip() + "\n\n" + f"subject of the chart: {query}"

        attempt = 0
        success = False

        while attempt < MAX_RETRIES and not success:
            try:
                query_response = agent.invoke(full_query)

                # 코드 추출
                code = extract_clean_code(query_response["output"], df_assign_code="df = df_table[i]").strip()

                if not code or "import" not in code:
                    raise ValueError("⚠️ 코드 블록이 출력에 포함되지 않았습니다.")

                # 저장 및 show 조치 포함한 코드 생성
                final_code, path = ensure_save_and_show(code, table_name[i], directory=os.path.abspath("./graph"))

                # 실행
                exec(final_code)
                print(f"[그래프 {i+1}] 저장 완료: {path}")
                print(final_code)
                success = True

            except Exception as e:
                attempt += 1
                print(f"⚠️ [표 {i+1}] 시도 {attempt} 실패: {e}")
                if attempt >= MAX_RETRIES:
                    print(f"❌ [표 {i+1}] 그래프 생성 실패: {query}")