"use strict";

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
class EtlOrchestrator {
    steps = [];
    constructor(steps) {
        this.steps = steps;
    }
    async executePipeline(initialPayload) {
        let currentPayload = initialPayload;
        let activeStepIdx = 0;
        console.log(`\n🌽 [Orchestrator] بدء سير عمل ETL لـ (Grain Silo) عبر ${this.steps.length} عُقد...`);
        try {
            for (activeStepIdx = 0; activeStepIdx < this.steps.length; activeStepIdx++) {
                const step = this.steps[activeStepIdx];
                console.log(`[Orchestrator] ⚙️ جاري تنفيذ: ${step.name}`);
                const startTime = Date.now();
                currentPayload = await step.execute(currentPayload);
                const endTime = Date.now(); 
                const executionTime = endTime - startTime;
                currentPayload.latencies[step.name] = executionTime;
                console.log(`   ⏱️ زمن التنفيذ: ${executionTime}ms`);
            }
            console.log(`[Orchestrator] ✅ اكتمل سير عمل الـ ETL بنجاح!`);
            console.log(`📊 تقرير الاختناقات (Latencies):`, currentPayload.latencies);
            return true;
        }
        catch (error) {
            console.error(`[Orchestrator] ❌ فشل في العُقدة '${this.steps[activeStepIdx].name}': ${error.message}`);
            await this.triggerRollback(activeStepIdx, currentPayload);
            return false;
        }
    }
    async triggerRollback(failedStepIdx, payload) {
        console.warn(`[Orchestrator] ⚠️ بدء الإجراءات التعويضية (Rollback Pulse)...`);
        for (let i = failedStepIdx - 1; i >= 0; i--) {
            const step = this.steps[i];
            console.warn(`[Orchestrator] ⏪ تراجع عن: ${step.name}`);
            try {
                await step.compensate(payload);
            }
            catch (rollbackError) {
                console.error(`[CRITICAL] فشل التراجع عن ${step.name}! حالة النظام غير مستقرة.`, rollbackError);
            }
        }
        console.info(`[Orchestrator] 🛡️ اكتمل التراجع. تم تنظيف التخزين المؤقت وحماية استقرار العنقود.`);
    }
}
const extractStep = {
    name: '1_Extract_Sensors',
    execute: async (payload) => {
        if (payload.simulateFailureAt === '1_Extract_Sensors')
            throw new Error('فشل قراءة المستشعرات (Time-out)');
        await sleep(40); 
        payload.logs.push('تم استخراج بيانات الرطوبة');
        return payload;
    },
    compensate: async (payload) => {
        console.log(`   -> [Extract] تفريغ ذاكرة المستشعرات المؤقتة (Clear Buffer).`);
    }
};
const transformStep = {
    name: '2_Transform_Validation',
    execute: async (payload) => {
        if (payload.simulateFailureAt === '2_Transform_Validation')
            throw new Error('خطأ في التحويل: بيانات رطوبة غير صالحة');
        await sleep(90); 
        payload.logs.push('تم تنقية وتحويل البيانات');
        return payload;
    },
    compensate: async (payload) => {
        console.log(`   -> [Transform] حذف السجلات غير المكتملة من خادم المعالجة.`);
    }
};
const loadStep = {
    name: '3_Load_Database',
    execute: async (payload) => {
        if (payload.simulateFailureAt === '3_Load_Database')
            throw new Error('انقطاع الاتصال بقاعدة البيانات الرئيسية');
        await sleep(30);
        payload.logs.push('تم الحفظ في قاعدة البيانات');
        return payload;
    },
    compensate: async (payload) => {
        console.log(`   -> [Load] التراجع عن الالتزام (Rollback DB Transaction).`);
    }
};
const aggregateStep = {
    name: '4_Aggregate_Analytics',
    execute: async (payload) => {
        if (payload.simulateFailureAt === '4_Aggregate_Analytics')
            throw new Error('فشل في خادم التجميع (Aggregation Node Failed)');
        await sleep(50);
        payload.logs.push('تم تجميع الإحصائيات اليومية');
        return payload;
    },
    compensate: async (payload) => {
        console.log(`   -> [Aggregate] مسح الإحصائيات المعلقة الخاصة بهذه الدفعة.`);
    }
};
async function runSimulation() {
    const pipelineSteps = [extractStep, transformStep, loadStep, aggregateStep];
    const orchestrator = new EtlOrchestrator(pipelineSteps);
    console.log("=================================================");
    console.log("اختبار 1: تشغيل DAG طبيعي (تحليل الاختناقات)");
    console.log("=================================================");
    const successBatch = {
        batchId: 'SILO-A-992',
        moistureLevel: 14.2,
        logs: [],
        latencies: {}
    };
    await orchestrator.executePipeline(successBatch);
    console.log("\n=================================================");
    console.log("اختبار 2: محاكاة فشل في قاعدة البيانات (تراجع شامل)");
    console.log("=================================================");
    const failingBatch = {
        batchId: 'SILO-B-104',
        moistureLevel: 18.5,
        logs: [],
        latencies: {},
        simulateFailureAt: '3_Load_Database' 
    };
    await orchestrator.executePipeline(failingBatch);
}
runSimulation();
