import dotenv from 'dotenv';
dotenv.config();

async function checkModels() {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey) {
        console.error("❌ 找不到 API Key，请检查 .env 文件！");
        return;
    }

    const url = `https://generativelanguage.googleapis.com/v1beta/models?key=${apiKey}`;
    console.log("📡 正在向 Google 服务器查询你这个 Key 可用的模型...");
    
    try {
        const response = await fetch(url);
        const data = await response.json();
        
        if (data.error) {
            console.error("❌ 查询失败:", data.error.message);
            return;
        }

        console.log("\n✅ 你的 API Key 支持以下模型 (请复制带 👉 的名字替换到 server.js 中):");
        data.models.forEach(m => {
            // 筛选出支持我们 generateContent 功能的模型
            if (m.supportedGenerationMethods && m.supportedGenerationMethods.includes("generateContent")) {
                console.log(`👉 "${m.name.replace('models/', '')}"`);
            }
        });
        console.log("\n");
    } catch (err) {
        console.error("网络请求失败，请检查网络代理:", err);
    }
}

checkModels();