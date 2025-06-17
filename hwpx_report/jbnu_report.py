from xml.etree.ElementTree import Element, SubElement
import xml.etree.ElementTree as ET
import json
from hwpx_report.jbnu_pydantic_file import Title  # Title 모델이 정의된 곳
from hwpx_report.hwp_xml import *
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

def clone_table_para_with_topic(template: ET.Element, topic_text: str, page_break: bool = False) -> ET.Element:
    p = deepcopy(template)

    # 두 번째 셀(<hp:tc>)의 <hp:t>을 찾아 텍스트 수정
    tc_elements = p.findall(".//hp:tbl//hp:tr//hp:tc", namespaces=NS)
    if len(tc_elements) >= 2:
        second_tc = tc_elements[1]
        t_elem = second_tc.find(".//hp:t", namespaces=NS)
        if t_elem is not None:
            print(f"✅ 기존 텍스트: {t_elem.text} → 새로운 텍스트: {topic_text.strip()}")
            t_elem.text = topic_text.strip()
        else:
            print("❌ <hp:t>를 찾지 못했습니다 (tc 내부)")
    else:
        print("❌ <hp:tc>가 2개 이상 존재하지 않습니다.")
        
    # if page_break:
    #     p.set("pageBreak", "1")

    return p


def extract(xml_path: str, para_ids: List[str]) -> (Dict[str, ET.Element], ET.ElementTree):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    templates = {}

    for pid in para_ids:
        candidates = root.findall(f".//hp:p[@paraPrIDRef='{pid}']", namespaces=NS)
        print(f"🔍 paraPrIDRef={pid} → 후보 개수: {len(candidates)}")

        matched = False
        for c in candidates:
            # ✅ 두 번째 <hp:tc> 셀 안의 텍스트로 topic 여부 판단
            tc_elements = c.findall(".//hp:tbl//hp:tr//hp:tc", namespaces=NS)
            if len(tc_elements) >= 2:
                second_tc = tc_elements[1]
                t_elem = second_tc.find(".//hp:t", namespaces=NS)
                if t_elem is not None and t_elem.text and "TOPIC" in t_elem.text.upper():
                    templates[pid] = copy.deepcopy(c)
                    print(f"✅ paraPrIDRef={pid} → topic 템플릿 확정 (텍스트 기반)")
                    matched = True
                    break

        # fallback 처리
        if not matched and candidates:
            templates[pid] = copy.deepcopy(candidates[0])
            print(f"⚠️ paraPrIDRef={pid} → fallback 템플릿 사용")

    return templates, tree


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

def copy_folder(src: str, dst: str):
    shutil.copytree(src, dst)
    print(f"✅ 폴더 복제 완료: {src} → {dst}")

# ✅ 전체 흐름
def process_jbnu_report(json_path: str, xml_path: str, save_path: str,sel_inc:str):
    print("process_report 시작")
    # 1. JSON 로드
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    parsed = Title(**data)
    print("json 로드 완료")

    # 2. 템플릿 불러오기 + 트리 구조
    template_ids = ["4","2","6", "11", "7"]  # 예: 32는 이미지용 추가
    templates, tree = extract(xml_path, template_ids)
    root = tree.getroot()
    row_template = find_table_row_template(xml_path, paraPr_id="7")
    tc_template = find_tc_template(xml_path, paraPr_id="7")
    print("템플릿 불러오기 + 트리구조 완료")

    # ✅ 기존 내용 제거
    for child in list(root):
        # 모든 하위 <hp:p> 탐색
        paras = child.findall(".//hp:p", namespaces=NS)
        keep = False
        for p in paras:
            para_id = p.attrib.get("paraPrIDRef", "")
            if para_id in {"4"}:
                keep = True 
                break
        if not keep:
            root.remove(child)
    print("기존 내용 제거 완료")



    # 3. Title 업데이트
    update_text_only(root, paraPrIDRef="4", new_text=parsed.title)   # Title 문단
    print("title 업데이트 완료")
    
 
    # 4. topic, sub_title, heading, content
    for topic_idx, topic in enumerate(parsed.topics):
        print(topic_idx)
        # 첫 topic이면 page_break=False, 나머지는 True
        is_first = topic_idx == 0

        if "2" in templates:
            filled = clone_table_para_with_topic(templates["2"], topic.topic, page_break=not is_first)
            root.append(filled)  # ✅ filled는 수정된 p 맞음

        for main in topic.main_points:
            if "6" in templates:
                root.append(clone_para(templates["6"], main.sub_title))

            for detail in main.details:
                if "11" in templates:
                    root.append(clone_para(templates["11"], detail.content))
                     
            if sel_inc in ["표", "표+그래프"]:
                for tbl in main.tables or []:
                    # 표 문단 복제
                    p_with_table = find_para_with_table(xml_path, paraPr_id="7")

                    # 캡션 및 행 삽입 
                    filled = fill_tbl_in_para(p_with_table, tbl.table, tbl.caption, row_template,tc_template,body_fill_id="4")

                    
                    parent = root.find(".//hp:body", NS) or root
                    parent.append(filled)
            
            
            if sel_inc in ["그래프", "표+그래프"]:
                for image in main.images or []:
                    p_with_image = find_para_with_image(xml_path, paraPr_id="7")
                    # 이미지 캡션 및 파일명 적용
                    filled = fill_pic_in_para(p_with_image, image.filename, image.caption)

                    # 문서에 추가
                    parent = root.find(".//hp:body", NS) or root
                    parent.append(filled)

    # 5. ✅ 전체 문단 줄바꿈 재생성  
    duplicate_lineseg_v2(root, max_width=75)
    print("줄바꿈 완료")
 
    # 5. 저장
    tree.write(save_path, encoding="utf-8", xml_declaration=True)
    print(f"\n✅ 최종 저장 완료: {save_path}")

# inc_list = ['없음','표','그래프','표+그래프']
# sel_inc = inc_list[0]


# ----------------- 실행 ------------------------

# 한글 보고서 복제
# copy_folder("template/JBNU보고서_최종", "hwpx_file/JBNU보고서_복사본")


# # 보고서 생성 실행  (json 파일, 양식.xml, 보고서 생성.xml)
# process_jbnu_report("json_file/hwpx_json_20250526.json", "jbnu_note.xml", "hwpx_file/JBNU보고서_복사본/Contents/section0.xml",sel_inc)


# # 수정된 보고서 압축 및 hwpx 변환 저장
# zip_as_hwpx("hwpx_file/JBNU보고서_복사본", "../test_hwp.hwpx")
# print("✅ 보고서 폴더 압축 완료")


# # ------------폴더 복제 및 수정 후 삭제 -----------------

# # 압축 후 폴더 삭제까지 하고 싶다면:
# shutil.rmtree("hwpx_file/JBNU보고서_복사본")