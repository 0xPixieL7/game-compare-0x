export default function IgdbAttribution({
    className = '',
}: {
    className?: string;
}) {
    return (
        <a
            href="https://www.igdb.com"
            target="_blank"
            rel="noopener noreferrer"
            className={`inline-flex items-center gap-2 opacity-75 transition-opacity hover:opacity-100 ${className}`}
            title="Data provided by IGDB"
        >
            <span className="text-[10px] font-bold tracking-widest text-muted-foreground uppercase">
                Powered by
            </span>
            <div className="flex items-center text-xl leading-none font-black tracking-tighter">
                <span className="text-[#9147ff]">IG</span>
                <span className="text-current">DB</span>
            </div>
        </a>
    );
}
