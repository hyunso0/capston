import requests
from langchain_core.output_parsers import PydanticOutputParser
from langchain.prompts import PromptTemplate
import json
from hwpx_report.jbnu_pydantic_file import Title  # 위에서 만든 모델
import re


from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage

# ChatOpenAI 초기화
llm = ChatOpenAI(
    base_url="",
    api_key="not-needed",
    model="Qwen3-14B",
    max_tokens=5000,
    streaming=False  # 요청에 따라 True로도 가능
)

def generate_response(prompt: str, system_message: str = "") -> str:
    """
    LangChain의 ChatOpenAI 객체를 사용해 프롬프트 응답을 생성하는 함수
    """
    try:
        messages = []

        if system_message:
            messages.append(SystemMessage(content=system_message))
        messages.append(HumanMessage(content=prompt))

        response = llm.invoke(messages)
        return response.content

    except Exception as e:
        return f"[에러 발생] {str(e)}"


json_analysis_for_hwp_parser = PydanticOutputParser(pydantic_object=Title)

# 프롬프트 템플릿
json_analysis_for_hwp_prompt = PromptTemplate(
    template="""
    You are an AI that structures Korean-language reports. Read the full free-form report content provided below and convert it into detailed JSON format **verbatim, without summarizing or paraphrasing**.

    Keep all original details **exactly as written**, including numbers, statistics, and descriptions.

    Tables must be preserved exactly as-is under the "table" field. All values must be strings, even if they are numbers (e.g., "652282").

    Each main point must include a 'sub_title'.  
    If the original content does not provide one, **generate a relevant Korean sub_title from the full context**. Never leave 'sub_title' empty.

    ---

    📸 **Image Caption Handling Rule**

    If a line like ![텍스트] or [텍스트] appears within a content block, treat it as an image.

    - Extract the actual text inside the brackets as the caption (e.g., if ![원인분석도], use "caption": "원인분석도").
    - Do **not** use a generic word like "설명".  
    - Then create an image object like this:

    json
        {{
            "caption": "텍스트",
            "filename": "텍스트.png",
            "type": "image"
        }}

    Insert the image object inside the 'images' field of the corresponding main point.

    ❗Important: Output must be valid JSON only.

    보고서 내용:
    {content}

    JSON 출력 형식 예시:
    {format_instructions} 
    """,
        input_variables=["content"],
        partial_variables={
            "format_instructions": json_analysis_for_hwp_parser.get_format_instructions()
        },
    )


def extract_json_block(text: str) -> str:
    """
    LLM 응답에서 가장 먼저 나오는 JSON 블록만 추출
    """
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        return match.group()
    else:
        raise ValueError("JSON 블록을 찾을 수 없습니다.")

def generate_structured_report(content: str, output_path: str = "test_qwen3.json") -> dict:
    print("json생성 시작")
    formatted_prompt = json_analysis_for_hwp_prompt.format(content=content)
    response = generate_response(prompt=formatted_prompt)

    print("=== LLM 응답 원문 ===")
    print(response)
    print("====================")
    try:
        response_json_str = extract_json_block(response)
        parsed = json_analysis_for_hwp_parser.parse(response)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(parsed.model_dump(), f, ensure_ascii=False, indent=2)
        print("✅ 구조화된 JSON 저장 완료")

        return parsed.model_dump()
    except Exception as e:
        raise RuntimeError(f"❌ JSON 파싱 실패: {e}")