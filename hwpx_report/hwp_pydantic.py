from typing import Literal, Optional, List
from pydantic import BaseModel, Field, constr 
from langchain_core.output_parsers import PydanticOutputParser

class TableBlock(BaseModel):
    caption: str 
    table: List[List[str]]
    type: Literal["table"] = "table"
    
class ImageContent(BaseModel):
    caption: str
    filename: str
    type: str  # "image"
    
class Detail(BaseModel):
    heading: str = Field(description="The heading for the detail section")  
    # heading: 각 세부 내용의 제목 (예: '서론', '결론' 같은 제목들)
    
    content: str = Field(description="The detailed content")
    # content: 해당 제목에 대한 실제 내용 (예: '서론 내용 설명' 같은 부분)

class MainContent(BaseModel):
    sub_title: str = Field(description="The title of the main content section")
    # sub_title: 주요 내용의 소제목 (예: '1장. 연구 배경')

    details: List[Detail] = Field(description="A list of detailed sections under the main content",default_factory=list)
    # details: 앞에서 만든 Detail 구조를 여러 개 넣을 수 있게 리스트 형태로 만들기
    
    tables: List[TableBlock] = Field(default_factory=list)  # ✅ 기본값 설정!
    
    images: Optional[List[ImageContent]] = []

class Summary(BaseModel):
    topic: str = Field(description="The main topic of this section")
    # topic: 전체 섹션의 주제 (예: '제안된 방법', '기술 개요' 같은 큰 주제)

    main_points: List[MainContent] = Field(description="A list of main content sections")
    # main_points: MainContent 구조를 리스트로 묶어서 여러 섹션을 포함할 수 있도록 설정
    
class Title(BaseModel):
    title: str = Field(description="The title of the content")
    # title: 문서 전체의 제목 

    summary: str = Field(description="A detailed summary of the content")
    # summary: 문서의 간략한 요약 
    
    topics: List[Summary] = Field(description="A list of topics with their main points and details")
    # topics: Summary 구조 리스트로 여러 주제와 섹션들을 담을 수 있게 만들기
    