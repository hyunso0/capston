from flask import Flask, request, jsonify
import requests
import os
from llm_agent.sql_report import run_sql_analysis
from llm_agent.graph import run_graph_generation
import matplotlib.pyplot as plt
from llm_agent.preprocess import preprocess_run
from flask import send_from_directory
from flask import Flask, request, Response, stream_with_context
import json
import time

app = Flask(__name__, static_url_path="/static", static_folder=os.path.abspath("data"))

@app.route("/")
def home():
    return "LLM Flask 서버가 실행 중입니다. /chat으로 POST 요청을 보내세요."

# ChatBot Code
@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json()
    prompt = data.get("prompt", "")

    def generate():
        try:
            print(f"[DEBUG-server.py] 사용자 프롬프트 수신: {prompt}")
            report_text, df_table, table_name = run_sql_analysis(prompt)

            print(f"[DEBUG-server.py] 보고서 생성 완료, 테이블 수: {len(df_table)}")

            for line in report_text.splitlines(): 
                yield f"data: {line}\n\n"
                time.sleep(0.3)

            # 🔥 분석 데이터 전송
            encoded_df_table = [df.to_json() for df in df_table]
            yield f"event: analysis\ndata: {json.dumps({'df_table': encoded_df_table, 'table_name': table_name})}\n\n"

            # 그래프 표시 여부 질문
            yield f"event: graph_query\ndata: {json.dumps({'table_name': table_name})}\n\n"
            yield "event: end\ndata: done\n\n"

        except Exception as e:
            yield f"data: ❌ 오류: {str(e)}\n\n"
            yield "event: end\ndata: done\n\n"

    return Response(stream_with_context(generate()), content_type="text/event-stream")


# File Upload Code
UPLOAD_FOLDER = os.path.abspath("./data/xlsx_data")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

@app.route("/upload", methods=["POST"])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error" : "파일이 없습니다."}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error" : "선택된 파일이 없습니다."}), 400

    try:
        file_path = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(file_path)
        print(f"[DEBUG] 파일 저장 위치: {file_path}")
        preprocess_run(file_path)
        return jsonify({"message": "파일 업로드 및 저장 성공", "filename": file.filename})
    except Exception as e:
        return jsonify({"error" : f"파일 저장 중 오류 발생 : {str(e)}"}), 500

@app.route('/static/graph/<path:filename>')
def serve_graph(filename):
    return send_from_directory(os.path.abspath('./graph'), filename)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port = 5000)