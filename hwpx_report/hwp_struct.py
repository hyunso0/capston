from xml.etree.ElementTree import Element, SubElement
import xml.etree.ElementTree as ET
import json
from hwpx_report.hwp_pydantic import Title  # Title 모델이 정의된 곳
from hwpx_report.hwp_xml import
from typing import Dict, List, Any
import copy
from copy import deepcopy
import unicodedata
import subprocess
import shutil

# 네임스페이스 설정
NS = {
    "hp": "http://www.hancom.co.kr/hwpml/2011/paragraph",
    'hc': 'http://www.hancom.co.kr/hwpml/2010/component'
    }
ET.register_namespace("hp", NS["hp"])
ET.register_namespace('hc', NS['hc'])

def clone_para_with_topic(template: ET.Element, topic_text: str, page_break: bool = False) -> ET.Element:
    p = deepcopy(template)

    # ✅ <hp:t> 텍스트 변경
    t_elem = p.find(".//hp:t", namespaces=NS)
    if t_elem is not None:
        print(f"✅ 기존 텍스트: {t_elem.text} → 새로운 텍스트: {topic_text.strip()}")
        t_elem.text = topic_text.strip()
    else:
        print("❌ <hp:t>를 찾지 못했습니다")

    # ✅ pageBreak 적용
    if page_break:
        p.set("pageBreak", "1")

    return p

def copy_folder(src: str, dst: str):
    shutil.copytree(src, dst)
    print(f"✅ 폴더 복제 완료: {src} → {dst}")

def zip_as_hwpx(source_folder: str, output_path: str):
    """
    source_folder 내부 내용을 압축하여 .hwpx 파일로 저장
    :param source_folder: 압축할 폴더 경로 (예: 'JBNU보고서_최종')
    :param output_path: 저장할 .hwpx 파일 경로 (예: '../final.hwpx')
    """
    result = subprocess.run(
        ["zip", "-r", output_path, "."],
        cwd=source_folder,  # ✅ 압축 대상 폴더 안에서 명령 실행
        check=True
    )
    print(f"✅ 압축 완료: {output_path}")


# ✅ 전체 흐름
def process_json_into_hwpx(json_path: str, xml_path: str, save_path: str,sel_inc:str):
    # 1. JSON 로드
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    parsed = Title(**data)

    # 2. 템플릿 불러오기 + 트리 구조
    template_ids = ["21", "28", "30", "25", "26", "29", "35", "34"]  # 예: 32는 이미지용 추가
    templates, tree = hwp_xml.extract_templates(xml_path, template_ids)
    root = tree.getroot()
    row_template = hwp_xml.find_table_row_template(xml_path, paraPr_id="35")
    tc_template = hwp_xml.find_tc_template(xml_path, paraPr_id="35")


    # ✅ 기존 내용 제거
    for child in list(root):
        # 모든 하위 <hp:p> 탐색
        paras = child.findall(".//hp:p", namespaces=NS)
        keep = False
        for p in paras:
            para_id = p.attrib.get("paraPrIDRef", "")
            if para_id in {"21", "28"}:
                keep = True 
                break
        if not keep:
            root.remove(child)


    # 3. Title, Summary 업데이트
    hwp_xml.update_text_only(root, paraPrIDRef="21", new_text=parsed.title)   # Title 문단
    hwp_xml.update_text_only(root, paraPrIDRef="28", new_text=parsed.summary) # Summary 문단
    
 
    # 4. topic, sub_title, heading, content
    for topic_idx, topic in enumerate(parsed.topics):
        # 첫 topic이면 page_break=False, 나머지는 True
        is_first = topic_idx == 0
        
        if "30" in templates: 
            root.append(clone_para_with_topic(templates["30"], topic.topic,page_break=not is_first))

        for main in topic.main_points:
            if "25" in templates:
                root.append(hwp_xml.clone_para(templates["25"], main.sub_title))

            for detail in main.details:
                if "26" in templates:
                    root.append(hwp_xml.clone_para(templates["26"], detail.heading))
                if "29" in templates:
                    root.append(hwp_xml.clone_para(templates["29"], detail.content))

            if sel_inc in ["표", "표+그래프"]:
                for tbl in main.tables:
                    # 표 문단 복제
                    p_with_table = hwp_xml.find_para_with_table(xml_path, paraPr_id="35")

                    # 캡션 및 행 삽입 
                    filled = hwp_xml.fill_tbl_in_para(p_with_table, tbl.table, tbl.caption, row_template,tc_template,body_fill_id="12")
                    
                    parent = root.find(".//hp:body", NS) or root
                    parent.append(filled)
            
            if sel_inc in ["그래프", "표+그래프"]:
                for image in main.images:
                    p_with_image = hwp_xml.find_para_with_image(xml_path, paraPr_id="34")
                    # 이미지 캡션 및 파일명 적용
                    filled = hwp_xml.fill_pic_in_para(p_with_image, image.filename, image.caption)

                    # 문서에 추가
                    parent = root.find(".//hp:body", NS) or root
                    parent.append(filled)

    # 5. ✅ 전체 문단 줄바꿈 재생성  
    hwp_xml.duplicate_lineseg_v2(root, max_width=75)
 
    # 5. 저장
    tree.write(save_path, encoding="utf-8", xml_declaration=True)
    print(f"\n✅ 최종 저장 완료: {save_path}")
 

inc_list = ['없음','표','그래프','표+그래프']
sel_inc = inc_list[0]


# ----------------- 실행 ------------------------

# 한글 보고서 복제
copy_folder("한글보고서_v2.0", "한글보고서_복사본")

# 실행  (json 파일, 양식.xml, 보고서 생성.xml)
process_json_into_hwpx("report_data.json", "note.xml", "한글보고서_복사본/Contents/section0.xml",sel_inc)


# 수정된 보고서 압축 및 hwpx 변환 저장
zip_as_hwpx("한글보고서_복사본", "../test_hwp.hwpx")
print("✅ 보고서 폴더 압축 완료")


# ------------폴더 복제 및 수정 후 삭제 -----------------

# 압축 후 폴더 삭제까지 하고 싶다면:
# shutil.rmtree("한글보고서_복사본")