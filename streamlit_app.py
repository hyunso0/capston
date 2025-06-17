import streamlit as st
import requests
import time
import os
import pandas as pd
from datetime import datetime
from streamlit_option_menu import option_menu
from streamlit_modal import Modal
from llm_agent.search import search_faiss_with_partial_and_similarity, load_components
from llm_agent.sql_report import run_sql_analysis
from llm_agent.graph import run_graph_generation
from hwpx_report.model_json import generate_structured_report
from hwpx_report.jbnu_report import *
import subprocess
import shutil
import sseclient
import requests
import json
import io

st.set_page_config(
    page_title="Team.단호박",
    page_icon="🎃",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ===== CSS 통합 =====
st.markdown("""
<style>
[data-testid="stSidebar"] {
    min-width: 830px;
}

/* 파일 업로드 부분 아래쪽 자동 목록 삭제 */
[data-testid="stFileUploaderDropzone"] + div {
    display: none !important;
}

/* 사이드바 강제 폭 제거 (접힘 상태에서 본문 확장 가능하도록) */
section[data-testid="stSidebar"][aria-expanded="false"] {
    width: 0 !important;
    min-width: 0 !important;
    max-width: 0 !important;
    visibility: hidden;
}


/* 보고서 리스트부분 마크다운 버튼화 */
/* 버튼을 감싸는 div 자체 간격 제거 */
div[data-testid="stButton"] {
    margin-bottom: 0rem !important;
    padding: 0rem !important;
}
div[data-testid="stButton"] > button {
    background-color: transparent !important;
    color: black !important;
    border: none !important;
    padding: 0.2rem 0rem !important;
    margin: 0rem !important;
    text-align: left !important;
    width: 100% !important;
    font-size: 1rem !important;
    font-weight: normal;
    display: block;
}

div[data-testid="stButton"] > button:hover {
    background-color: #f2f2f2 !important;
    cursor: pointer;
}

main > div:first-child {
    padding-top: 2.5rem;
}
</style>
""", unsafe_allow_html=True)

# ===== side bar ======
with st.sidebar:
    st.image("logo1.png", width=180)
    # 상태 초기화
    if "search_input" not in st.session_state:
        st.session_state.search_input = ""
    if "search_results" not in st.session_state:
        st.session_state.search_results = []
    if "uploaded_files" not in st.session_state:
        st.session_state["uploaded_files"] = []
    if "selected_reports" not in st.session_state:
        st.session_state.selected_reports = []
    if "selected_preview_file" not in st.session_state:
        st.session_state.selected_preview_file = None

    # 리스트 초기화
    selected_list = []

# 검색 콜백
    def on_search():
        keyword = st.session_state["search_input"].strip()
        if keyword:
            model, index, meta, file_token_index = load_components()
            results = search_faiss_with_partial_and_similarity(
                keyword, model, index, meta, file_token_index
            )
            st.session_state.search_results = [r['file'] for r in results]
        else:
            st.session_state.search_results = []

    # UI
    st.text_input(
        label="",
        placeholder="파일에서 검색할 키워드를 입력하세요",
        key="search_input",
        on_change=on_search
    )

    # 결과 출력
    with st.container(height=200):
        if st.session_state.search_results:
            st.markdown(f"📌 '{st.session_state.search_input}'에 대한 결과입니다:")
            for result in st.session_state.search_results:
                checked = st.checkbox(result, key=f"chk_{result}")
                if checked and result not in st.session_state.selected_reports:
                    st.session_state.selected_reports.append(result)
                elif not checked and result in st.session_state.selected_reports:
                    st.session_state.selected_reports.remove(result)
            # 디버깅 출력용 리스트 생성
            selected_list = list(st.session_state.selected_reports)
            print("📌 현재 선택된 보고서 리스트:", selected_list)

        else:
            st.markdown("*검색 결과가 없습니다.*")


    st.markdown("## 📂 사용할 데이터 파일")

    # 파일 업로드
    with st.expander("📂 파일 업로드", expanded=False):
        uploaded_files = st.file_uploader(
            " ", 
            type=["xlsx", "csv"], 
            accept_multiple_files=True
        )

        if uploaded_files:
            for file in uploaded_files:
                if file.name not in st.session_state["uploaded_files"]:
                    files = {"file": (file.name, file, file.type)}
                    try:
                        response = requests.post("http://localhost:5000/upload", files=files)
                        if response.status_code == 200:
                            st.session_state["uploaded_files"].append(file.name)
                            if file.name not in st.session_state["selected_reports"]:
                                st.session_state["selected_reports"].append(file.name)
                        else:
                            st.error(f"❌ {file.name} 업로드 실패")
                    except Exception as e:
                        st.error(f"❌ 서버 오류: {e}")

    # ===== 보고서 리스트 박스 + 매핑 =====
    csv_dir = "./data/csv_data"
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith(".csv")]

    file_display_map = {}

    # 1. csv_data 폴더 내 파일들 (확장자 제거 후 등록)
    for f in csv_files:
        name_without_ext = os.path.splitext(f)[0]
        file_display_map[name_without_ext] = f

    # 2. 업로드된 파일들도 등록 (확장자 포함 이름 그대로)
    for f in st.session_state["uploaded_files"]:
        if f.endswith(".xlsx"):
            f_csv = f.replace(".xlsx", ".csv")
            file_display_map[f] = f_csv  # xlsx 이름 → 실제 저장된 csv 이름으로 매핑
        else:
            file_display_map[f] = f

    # 보고서 리스트 박스
    # with st.container(height=170):
    #     if st.session_state["uploaded_files"] or st.session_state["selected_reports"]:
    #         for fname in st.session_state["uploaded_files"]:
    #             if st.button(fname, key=f"btn_upload_{fname}"):
    #                 st.session_state.selected_preview_file = fname
    #         for selected in st.session_state["selected_reports"]:
    #             if st.button(selected, key=f"btn_select_{selected}"):
    #                 st.session_state.selected_preview_file = selected
    #     else:
    #         st.markdown("*선택된 파일이 없습니다.*")
    with st.container(height=170):
        if st.session_state["uploaded_files"] or st.session_state["selected_reports"]:
            # ✅ uploaded_files 먼저 보여줌
            for fname in st.session_state["uploaded_files"]:
                if st.button(fname, key=f"btn_upload_{fname}"):
                    st.session_state.selected_preview_file = fname

            # ✅ selected_reports 중, uploaded에 없는 항목만 추가로 보여줌
            for selected in st.session_state["selected_reports"]:
                if selected not in st.session_state["uploaded_files"]:  # ✅ 중복 제거
                    if st.button(selected, key=f"btn_select_{selected}"):
                        st.session_state.selected_preview_file = selected
        else:
            st.markdown("*선택된 파일이 없습니다.*")
    
    # ===== 데이터 결과 출력 =====
    st.markdown("## 📊 데이터 파일 미리보기")

    if st.session_state.get("selected_preview_file"):
        display_name = st.session_state.selected_preview_file
        real_fname = file_display_map.get(display_name)

        if real_fname:
            search_dirs = ["./data/csv_data"]
            file_path = None

            for d in search_dirs:
                test_path = os.path.join(d, real_fname)
                if os.path.exists(test_path):
                    file_path = test_path
                    break

            if file_path:
                try:
                    if file_path.endswith(".csv"):
                        df = pd.read_csv(file_path)
                    elif file_path.endswith(".xlsx"):
                        df = pd.read_excel(file_path)
                    else:
                        st.warning("미리보기 지원되지 않는 파일 형식입니다.")
                        df = None

                    if df is not None:
                        st.markdown(f"📋 **'{real_fname}' 미리보기 (상위 5행)**")
                        st.dataframe(df.head(5), use_container_width=True)
                except Exception as e:
                    st.error(f"❌ 파일 로드 실패: {e}")
            else:
                st.warning(f"⚠️ '{real_fname}' 파일을 csv_data나 custom_data에서 찾을 수 없습니다.")
        else:
            st.warning(f"⚠️ '{display_name}' 이름으로 매핑되는 실제 파일명을 찾을 수 없습니다.")


# ========================= 챗봇 ==============================
#st.subheader("💬 LLM ChatBot")

# 채팅 기록 저장
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "step" not in st.session_state:
    st.session_state.step = "option"
if "template_selected" not in st.session_state:
    st.session_state.template_selected = False
if "include_table" not in st.session_state:
    st.session_state.include_table = False
if "include_graph" not in st.session_state:
    st.session_state.include_graph = False
if "expand_report_ui" not in st.session_state:
    st.session_state.expand_report_ui = True
if "report_expanded" not in st.session_state:
    st.session_state.report_expanded = True
if "graph_choice_made" not in st.session_state:
    st.session_state.graph_choice_made = False
if "graph_generate_clicked" not in st.session_state:
    st.session_state.graph_generate_clicked = False


st.markdown("""
    <style>
    .chat-box {
        border: 2px solid #cccccc;
        border-radius: 12px;
        height: 400px;
        padding: 1rem;
        overflow-y: auto;
        background-color: transparent;
    }
    </style>
""", unsafe_allow_html=True)

# 채팅 박스 내부에서 메시지를 출력
with st.container():
    # 메시지를 chat_message로 출력 (박스 안에서 실행되도록 보장)
    for message in st.session_state.chat_history:
        with st.chat_message("user"):
            st.markdown(message["user"])
        with st.chat_message("assistant"):
            st.markdown(message["assistant"])

    st.markdown('</div>', unsafe_allow_html=True)

# 입력창은 밖에 고정
if prompt := st.chat_input("질문을 입력하세요"):
    # 사용자 메시지 먼저 넣고, 서버에 응답 요청
    with st.chat_message("user"):
        st.markdown(prompt)

    # 응답 대기
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        with st.spinner("AI가 응답 중입니다..."):
            try:
                response_stream = requests.post(
                    "http://localhost:5000/chat",
                    json={"prompt": prompt},
                    stream=True
                )
                client = sseclient.SSEClient(response_stream)

                full_response = ""
                st.session_state.graph_table_name = None
                st.session_state.graph_df_table = None
                
                for event in client.events():
                    if event.event == "analysis":
                        # ✅ 분석 데이터 수신 및 저장
                        data = json.loads(event.data)
                        st.session_state.latest_df_table = [
                            pd.read_json(io.StringIO(js)) for js in data["df_table"]
                        ]
                        st.session_state.latest_table_names = data["table_name"]

                    elif event.event == "graph_query":
                        st.session_state.graph_table_name = json.loads(event.data)["table_name"]

                    elif event.event == "end":
                        break

                    else:
                        full_response += event.data + "\n"
                        message_placeholder.write(full_response)

                message_placeholder.markdown(full_response.strip(), unsafe_allow_html=True)

                st.session_state.chat_history.append({
                    "user": prompt,
                    "assistant": full_response
                })

            except Exception as e:
                message_placeholder.markdown(f"❌ 예외 발생: {e}")
                st.session_state.chat_history.append({
                    "user": prompt,
                    "assistant": f"❌ 예외 발생: {e}"
                })
    

# if st.session_state.get("graph_table_name") and not st.session_state.get("graph_choice_made", False):
#     st.subheader("📊 그래프를 생성하시겠습니까?")
#     col1, col2 = st.columns(2)

#     with col1:
#         if st.button("✅ 예 (그래프 생성)", key="yes_generate_graph_main"):
#             st.session_state.graph_choice_made = True

#             with st.spinner("🛠️ 그래프 생성 중입니다..."):
#                 time.sleep(0.1)
#                 run_graph_generation(
#                     st.session_state.latest_df_table,
#                     st.session_state.latest_table_names
#                 )

#             graph_paths = [
#                 f"/static/graph/{name}.png"
#                 for name in st.session_state.latest_table_names
#             ]
#             st.session_state.graph_paths = graph_paths
#             st.session_state.last_graph_paths = graph_paths
#             st.session_state.report_expanded = False
#             st.session_state.graph_table_name = None

#             st.rerun()

#     with col2:
#         if st.button("❌ 아니요", key="no_generate_graph_main"):
#             st.session_state.graph_choice_made = True
#             st.session_state.graph_table_name = None
#             st.session_state.report_expanded = False
#             st.rerun()

# ✅ UI 출력 조건
if st.session_state.get("graph_table_name") and not st.session_state.get("graph_choice_made", False):
    st.subheader("📊 그래프를 생성하시겠습니까?")
    col1, col2 = st.columns(2)

    with col1:
        if st.button("✅ 예 (그래프 생성)", key="yes_generate_graph_main"):
            # ✅ 상태만 설정하고 즉시 종료 → 아래 그래프 생성 코드만 실행됨
            st.session_state.graph_choice_made = True
            st.query_params["_"] = str(time.time())
            st.stop()

    with col2:
        if st.button("❌ 아니요", key="no_generate_graph_main"):
            st.session_state.graph_choice_made = True
            st.session_state.graph_table_name = None
            st.session_state.report_expanded = False
            st.query_params["_"] = str(time.time())
            st.stop()

# ✅ 선택 후 실행: UI 중단되고 여기서 바로 그래프 생성만 실행됨
if st.session_state.get("graph_choice_made") and st.session_state.get("graph_table_name"):
    with st.spinner("🛠️ 그래프 생성 중입니다..."):
        time.sleep(0.1)
        run_graph_generation(
            st.session_state.latest_df_table,
            st.session_state.latest_table_names
        )

    graph_paths = [
        f"/static/graph/{name}.png"
        for name in st.session_state.latest_table_names
    ]
    st.session_state.graph_paths = graph_paths
    st.session_state.last_graph_paths = graph_paths
    st.session_state.report_expanded = False
    st.session_state.graph_table_name = None

    # ✅ 후처리 후 정상 UI 복귀
    st.session_state.graph_choice_made = False
    st.rerun()

#======================= 버튼 바로 위쪽에 작은 창 나옴 ====================
# 한글 보고서 생성 버튼 : 응답 나온 이후에만 나옴
generate = False

if (
    st.session_state.chat_history and 
    st.session_state.chat_history[-1].get("assistant", "").strip() and
    st.session_state.graph_table_name is None  # ✅ Y/N 선택이 끝났을 때만 보고서 생성기 등장
    ):
    if st.session_state.get("last_graph_paths"):
        cols = st.columns(2)  # ✅ 추가
        for i, path in enumerate(st.session_state.last_graph_paths):  # ✅ enumerate 추가
            with cols[i % 2]:  # 2열로 나눠 표시
                st.image(
                    f"http://localhost:5000{path}",
                    caption=os.path.basename(path).replace(".png", ""),
                    width=400
                )

    if st.session_state.get("report_ready") and st.session_state.report_expanded:
        st.session_state.report_expanded = False

    with st.expander("📄 한글 보고서 생성하기", expanded=st.session_state.report_expanded):  # popover 대신 expander 사용
        if st.session_state.step == "option":
            st.subheader("1️⃣ 포함할 항목을 선택하세요")
            st.caption("보고서에 포함할 요소를 선택하세요. 이후 양식 선택 단계로 넘어갑니다.")
            st.session_state.include_table = st.checkbox("표 포함", value=st.session_state.include_table)
            st.session_state.include_graph = st.checkbox("그래프 포함", value=st.session_state.include_graph)

            st.session_state.report_expanded = True

            # 포함 항목 처리 (표, 그래프)
            inc_list = ['없음','표','그래프','표+그래프']
            sel_inc = inc_list[
                (2 if st.session_state.include_graph else 0) +
                (1 if st.session_state.include_table else 0)
            ]

            st.session_state["sel_inc"] = sel_inc

            st.divider()

            if st.button("다음"):
                st.session_state.step = "template"

        elif st.session_state.step == "template":
            sel_inc = st.session_state.get("sel_inc", "없음")

            st.subheader("2️⃣ 보고서 양식을 선택하세요")
            st.caption("선택한 양식에 따라 보고서의 형태가 달라집니다.")

            template_options = [
                "JBNU 보고서 양식",
                "일반 보고서 양식"
            ]
            selected_template = st.radio(
                "",
                template_options,
                key="template_choice"
            )

            st.session_state.template_selected = True
            st.divider()

            generate_disabled = not st.session_state.template_selected
            generate = st.button("✅ 한글 보고서 생성", disabled=generate_disabled)
    
            if generate:
                try:
                    #마지막 assistant 응답 텍스트 가져오기
                    raw_response = st.session_state.chat_history[-1].get("assistant", "").strip()

                    today_str = datetime.today().strftime("%Y%m%d")
                    output_dir = "hwpx_report/json_file"
                    os.makedirs(output_dir, exist_ok=True)

                    output_path = os.path.join(output_dir, "final_0611.json")
                    result_json = generate_structured_report(content=raw_response, output_path=output_path)
                    #st.success("✅ 구조화된 JSON 보고서가 생성되었습니다.")
                    #st.json(result_json, expanded=False)

                    # 한글 보고서 복제
                    copy_folder("hwpx_report/template/JBNU보고서_최종", "hwpx_report/hwpx_file/JBNU보고서_복사본")

                    # ✅ 그래프 파일 복사
                    graph_dir = os.path.abspath("./graph")  # 실제 파일 저장된 폴더
                    target_bin_dir = os.path.abspath("./hwpx_report/hwpx_file/JBNU보고서_복사본/BinData")
                    os.makedirs(target_bin_dir, exist_ok=True)

                    for graph_path in st.session_state.get("graph_paths", []):
                        filename = os.path.basename(graph_path)  # ex: chart1.png
                        src_path = os.path.join(graph_dir, filename)  # 실제 파일 경로
                        dst_path = os.path.join(target_bin_dir, filename)

                        try:
                            shutil.copy(src_path, dst_path)
                            print(f"✅ 그래프 복사 완료: {filename}")
                        except Exception as e:
                            print(f"❌ 그래프 복사 실패: {filename}, 이유: {e}")



                    # 보고서 생성 실행  (json 파일, 양식.xml, 보고서 생성.xml)
                    process_jbnu_report(
                        "hwpx_report/json_file/final_0611.json", 
                        # output_path,
                        "hwpx_report/jbnu_note.xml", 
                        "hwpx_report/hwpx_file/JBNU보고서_복사본/Contents/section0.xml",
                        sel_inc
                    )

                    # --------------------------------------------------------------------------------------
                    import os
                    import xml.etree.ElementTree as ET
                    from typing import List
                    def register_images_to_content_hpf(content_hpf_path: str, bindata_dir: str):
                        tree = ET.parse(content_hpf_path)
                        root = tree.getroot()

                        # 직접 prefix 없이 {namespace} 방식으로 탐색
                        manifest_node = root.find('{http://www.idpf.org/2007/opf/}manifest')
                        if manifest_node is None:
                            raise ValueError("❌ <opf:manifest> 태그를 content.hpf에서 찾을 수 없습니다.")

                        # 이미지 파일 리스트 추출
                        image_files = [f for f in os.listdir(bindata_dir) if f.endswith(('.png', '.jpg', '.jpeg'))]

                        # 각 이미지 파일을 <manifest>에 등록
                        for image in image_files:
                            href = f"BinData/{image}"
                            media_type = "image/png" if image.endswith(".png") else "image/jpeg"
                            image_id = os.path.splitext(image)[0]

                            # 중복 체크: 모든 <item> 순회
                            already_exists = any(item.get("href") == href for item in manifest_node.findall('{http://www.idpf.org/2007/opf/}item'))
                            if not already_exists:
                                new_item = ET.SubElement(manifest_node, '{http://www.idpf.org/2007/opf/}item')
                                new_item.set("id", image_id)
                                new_item.set("href", href)
                                new_item.set("media-type", media_type)
                                new_item.set("isEmbeded", "1")
                                print(f"✅ 등록됨: {href}")
                            else:
                                print(f"⚠️ 이미 존재함: {href}")

                        # 변경 내용 저장
                        tree.write(content_hpf_path, encoding="utf-8", xml_declaration=True)



                    register_images_to_content_hpf(
                        content_hpf_path="hwpx_report/hwpx_file/JBNU보고서_복사본/Contents/content.hpf",
                        bindata_dir="hwpx_report/hwpx_file/JBNU보고서_복사본/BinData"
                    )
                    # --------------------------------------------------------------------------------------

                    # 수정된 보고서 압축 및 hwpx 변환 저장
                    zip_as_hwpx("hwpx_report/hwpx_file/JBNU보고서_복사본", "../final_0611.hwpx")
                    print("✅ 보고서 폴더 압축 완료")


                    # ------------폴더 복제 및 수정 후 삭제 -----------------
                    # 압축 후 폴더 삭제까지 하고 싶다면:
                    shutil.rmtree("hwpx_report/hwpx_file/JBNU보고서_복사본")

                    st.session_state.report_ready = True  # ✅ 상태 저장
                    st.session_state.hwpx_path = "hwpx_report/hwpx_file/final_0611.hwpx"  # 경로 저장
                    st.session_state.report_expanded = False # expander 접힘

                    # if st.session_state.get("report_ready") and os.path.exists(st.session_state.hwpx_path):
                    #     st.subheader("3️⃣ 한글 보고서를 다운로드하세요")
                    #     with open(st.session_state.hwpx_path, "rb") as f:
                    #         st.download_button(
                    #             label="📥 HWPX 파일 다운로드",
                    #             data=f,
                    #             file_name="보고서.hwpx",
                    #             mime="application/octet-stream",
                    #             key="download_report_button"
                    #         )

                except Exception as e:
                    if os.path.exists("hwpx_report/hwpx_file/JBNU보고서_복사본"):
                        shutil.rmtree("hwpx_report/hwpx_file/JBNU보고서_복사본")
                    st.error(f"❌ 보고서 생성 중 오류 발생: {e}")


    if st.session_state.get("report_ready") and os.path.exists(st.session_state.hwpx_path):
        st.subheader("한글보고서 생성이 완료되었습니다.")
        with open(st.session_state.hwpx_path, "rb") as f:
            st.download_button(
                label="📥 HWPX 파일 다운로드",
                data=f,
                file_name="보고서.hwpx",
                mime="application/octet-stream",
                key="download_report_button"
            )