from xml.etree.ElementTree import Element, SubElement
import xml.etree.ElementTree as ET
import json
from hwpx_report.jbnu_pydantic_file import Title  # Title 모델이 정의된 곳
from typing import Dict, List, Any
import copy
from copy import deepcopy
import unicodedata
from pathlib import Path

# 네임스페이스 설정
NS = {
    "hp": "http://www.hancom.co.kr/hwpml/2011/paragraph",
    'hc': "http://www.hancom.co.kr/hwpml/2011/core"
    }
ET.register_namespace("hp", NS["hp"])
ET.register_namespace('hc', NS['hc'])    


# -----------  줄바꿈을 위해 텍스트 길이 측정 함수   ---------------
def calculate_textpos_by_width(text: str, max_width: float = 75.0) -> list:
    positions = [0]
    current_width = 0.0

    for idx, char in enumerate(text):
        if 'HANGUL' in unicodedata.name(char, ''):
            char_width = 2.0
        elif char == ' ':
            char_width = 0.5
        else:
            char_width = 1.0

        if current_width + char_width > max_width:
            positions.append(idx)
            current_width = char_width
        else:
            current_width += char_width

    return positions

# -----------  줄바꿈 함수   ---------------
def duplicate_lineseg_v2(root: ET.Element, max_width: float = 75):
    """글자 폭 누적 기준으로 정확한 줄바꿈과 linesegarray 생성"""

    for p_elem in root.findall(".//hp:p", namespaces=NS):
        t_elem = p_elem.find(".//hp:t", namespaces=NS)
        if t_elem is None or t_elem.text is None:
            continue

        text = t_elem.text.strip()
        if len(text) <= 1:
            continue

        linesegarray = p_elem.find(".//hp:linesegarray", namespaces=NS)
        if linesegarray is None:
            continue

        original_lineseg = linesegarray.find(".//hp:lineseg", namespaces=NS)
        if original_lineseg is None:
            continue

        # ✅ 기본 속성 복사
        base_attrs = original_lineseg.attrib.copy()
        base_vertpos = int(base_attrs.get("vertpos", "20514"))

        # ✅ 줄바꿈 위치 계산
        textpos_list = calculate_textpos_by_width(text, max_width=max_width)

        # ✅ 기존 줄 삭제
        linesegarray.clear()

        for i, textpos in enumerate(textpos_list):
            new_lineseg = ET.Element("hp:lineseg")
            new_attrs = base_attrs.copy()
            new_attrs["textpos"] = str(textpos)
            new_attrs["vertpos"] = str(base_vertpos + i * 2160)
            new_attrs["flags"] = "2490368" if i == 0 else "1441792"
            new_lineseg.attrib.update(new_attrs)

            linesegarray.append(new_lineseg)

    print(f"✅ 모든 문단 linesegarray 재생성 완료.")
    
    
# -----------   title, summary 텍스트 수정 함수 ---------------
def update_text_only(root: ET.Element, paraPrIDRef: str, new_text: str):
    """특정 paraPrIDRef 문단 찾아 텍스트만 교체 (줄바꿈은 마지막에 따로)"""
    p_elem = root.find(f".//hp:p[@paraPrIDRef='{paraPrIDRef}']", namespaces=NS)
    if p_elem is not None:
        t_elem = p_elem.find(".//hp:t", namespaces=NS)
        if t_elem is not None:
            t_elem.text = new_text.strip()
            print(f"✅ paraPrIDRef={paraPrIDRef} 텍스트 업데이트 완료.")
    else:
        print(f"⚠️ paraPrIDRef={paraPrIDRef} 문단을 찾을 수 없습니다.")
        
        
# -----------   table 양식 찾고 복제하는 함수 ---------------
def find_para_with_table(note_path: str, paraPr_id: str = "35") -> ET.Element:
    tree = ET.parse(note_path)
    root = tree.getroot()

    for p in root.findall(".//hp:p", NS):
        if p.attrib.get("paraPrIDRef") == paraPr_id and p.find(".//hp:tbl", NS) is not None:
            return deepcopy(p)
    raise ValueError(f"paraPrIDRef={paraPr_id}를 가진 <hp:p> 안에 <hp:tbl>이 없습니다.")


# -----------   table의 hp:tr(행) 양식 복제하는 함수 ---------------
def find_table_row_template(note_path: str, paraPr_id: str = "35") -> ET.Element:
    """
    note.xml에서 특정 paraPr_id를 가진 <hp:tbl> 내 <hp:tr>을 복제
    """
    tree = ET.parse(note_path)
    root = tree.getroot()
    
    tbl = find_para_with_table(note_path, paraPr_id=paraPr_id)
    tr_template = tbl.find(".//hp:tr", NS)
    if tr_template is None:
        raise ValueError("❌ <hp:tr>를 <hp:tbl> 안에서 찾을 수 없습니다.")

    return deepcopy(tr_template)

# -----------   table의 hp:tc(열) 양식 복제하는 함수 ---------------
def find_tc_template(note_path: str, paraPr_id: str = "35") -> ET.Element:
    """
    note.xml에서 특정 paraPr_id를 가진 <hp:tbl> 내 <hp:tc>를 복제
    """
    tree = ET.parse(note_path)
    root = tree.getroot()
    
    tbl = find_para_with_table(note_path, paraPr_id=paraPr_id)
    tc = tbl.find(".//hp:tc", NS)
    if tc is None:
        raise ValueError("❌ <hp:tc>를 <hp:tbl> 안에서 찾을 수 없습니다.")

    return deepcopy(tc)

# -------- <hp:caption> 내부의 <hp:t> 캡션 텍스트만 바꿔주는 함수 -------
def update_caption_text(caption_block: ET.Element, new_text: str):
    """
    <hp:caption> 내부의 <hp:t> 캡션 텍스트만 바꿔주는 함수
    """
    para = caption_block.find(".//hp:subList/hp:p", NS)
    if para is None:
        raise ValueError("❌ <hp:p>를 <hp:caption> 내부에서 찾을 수 없습니다.")

    run = para.find("hp:run", NS)
    if run is None:
        raise ValueError("❌ <hp:run>이 없습니다.")

    t_list = run.findall("hp:t", NS)
    if len(t_list) >= 2:
        t_list[1].text = " " + new_text  # 표 번호 뒤에 텍스트
    elif len(t_list) == 1:
        t_list[0].text = " " + new_text  # 번호가 없는 경우
    else:
        raise ValueError("❌ <hp:t>가 없습니다.")

# ----------- 표 생성하는 함수 ---------
def fill_tbl_in_para(
    p_elem: ET.Element,
    table_data: List[List[str]],
    caption_text: str,
    row_template: ET.Element,
    tc_template: ET.Element,
    body_fill_id: str = "4"  # 추가: 기본값은 기존과 동일
    ) -> ET.Element:
    # ... 캡션은 이전 방식 그대로 ...

    tbl = p_elem.find(".//hp:tbl", NS)
    for tr in tbl.findall("hp:tr", NS):
        tbl.remove(tr)

    # ✅ colCnt, rowCnt 자동 설정 (꼭 필요!)
    tbl.set("rowCnt", str(len(table_data)))
    tbl.set("colCnt", str(max(len(row) for row in table_data)))
    
    update_caption_text(tbl, caption_text)
    
    # 3. 행 삽입
    for row_idx, row in enumerate(table_data):
        new_tr = deepcopy(row_template)

        # 기존 셀 모두 삭제
        for tc in new_tr.findall(".//hp:tc", NS):
            new_tr.remove(tc)

        # 열 삽입
        for col_idx, cell_text in enumerate(row):
            new_tc = deepcopy(tc_template)
            
            if row_idx != 0:
                new_tc.set("borderFillIDRef", body_fill_id)  # 매개변수로 대체 

            # ✅ <hp:cellAddr> 행렬 좌표 수정
            cell_addr = new_tc.find("hp:cellAddr", NS)
            if cell_addr is not None:
                cell_addr.set("colAddr", str(col_idx))
                cell_addr.set("rowAddr", str(row_idx))
 
            # ✅ <hp:t> 텍스트 삽입
            t_elem = new_tc.find(".//hp:t", NS)
            if t_elem is not None:
                t_elem.text = cell_text

            new_tr.append(new_tc)

        tbl.append(new_tr)
    return p_elem

# ✅ 템플릿 요소 추출
def extract_templates(xml_path: str, para_ids: List[str]) -> (Dict[str, ET.Element], ET.ElementTree):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    templates = {}

    for pid in para_ids:
        found = root.find(f".//hp:p[@paraPrIDRef='{pid}']", namespaces=NS)
        if found is not None:
            templates[pid] = copy.deepcopy(found)

    return templates, tree

# ✅ 문단 복제 + 텍스트 삽입
def clone_para(template: ET.Element, text: str) -> ET.Element:
    p = copy.deepcopy(template)
    t_elem = p.find(".//hp:t", namespaces=NS)
    if t_elem is not None:
        t_elem.text = text.strip()
    return p


# --------------------- 그래프 이미지 생성 함수 -------------------------------
 
# 이미지 캡션 수정
def update_caption_in_para(p_with_image: ET.Element, caption: str) -> ET.Element:
    """
    이미지 문단 내 <hp:run> 블록에서 실제 캡션(<hp:t> 그래프입니다.)을 주어진 caption으로 바꾼다.
    """
    runs = p_with_image.findall(".//hp:run", NS)

    for run in runs:
        # <hp:ctrl>을 포함하는 run에서 텍스트 노드를 찾는다
        ctrl = run.find("hp:ctrl", NS)
        if ctrl is not None:
            texts = run.findall("hp:t", NS)
            if len(texts) >= 2:
                # 두 번째 <hp:t>를 caption으로 대체
                texts[1].text = f" {caption}"
                break

    return p_with_image

# 이미지 문단 템플릿 찾기
def find_para_with_image(note_path: str, paraPr_id: str = "34") -> ET.Element:
    tree = ET.parse(note_path)
    root = tree.getroot()

    for p in root.findall(".//hp:p", NS):
        if p.attrib.get("paraPrIDRef") == paraPr_id and p.find(".//hp:pic", NS) is not None:
            return deepcopy(p)

    raise ValueError(f"<hp:pic>이 포함된 paraPrIDRef={paraPr_id} 문단을 찾을 수 없습니다.")

# 이미지 수정 함수
def fill_pic_in_para(p_with_image: ET.Element, binary_id: str, caption: str) -> ET.Element:
    image_ref = Path(binary_id).stem  # "image4.jpg" → "image4"
    
    for elem in p_with_image.iter():
        print(elem.tag)

    img_tag = p_with_image.find(".//hc:img", NS)  # ns2:img → hc:img
    if img_tag is not None:
        img_tag.set("binaryItemIDRef", image_ref)
    else:
        raise ValueError("이미지 태그(<hc:img>)를 찾을 수 없습니다.")


    p_with_image = update_caption_in_para(p_with_image, caption)

    comment_tag = p_with_image.find(".//hp:shapeComment", NS)
    if comment_tag is not None:
        comment_tag.text = (
            "그림입니다.\n"
            f"원본 그림의 이름: {binary_id}\n"
            "원본 그림의 크기: 가로 640pixel, 세로 426pixel\n"
            "색 대표 : sRGB\n"
            "EXIF 버전 : 0221"
        )

    return p_with_image